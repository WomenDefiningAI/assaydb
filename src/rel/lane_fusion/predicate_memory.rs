//! Storage and extra string workspace for the total fused-predicate subset.
use crate::{
    Result,
    expr::{Expr, Kind, Literal},
    types::Type,
};
use arrow::record_batch::RecordBatch;

/// A conservative byte bound evaluated without inspecting individual rows.
#[derive(Clone, Copy, Default)]
pub(super) struct Scratch {
    fixed: usize,
    per_row: usize,
    per_input_byte: usize,
}

impl Scratch {
    fn add(self, other: Self) -> Option<Self> {
        Some(Self {
            fixed: self.fixed.checked_add(other.fixed)?,
            per_row: self.per_row.checked_add(other.per_row)?,
            per_input_byte: self.per_input_byte.checked_add(other.per_input_byte)?,
        })
    }

    fn scale(self, factor: usize) -> Option<Self> {
        Some(Self {
            fixed: self.fixed.checked_mul(factor)?,
            per_row: self.per_row.checked_mul(factor)?,
            per_input_byte: self.per_input_byte.checked_mul(factor)?,
        })
    }

    pub(super) fn is_empty(self) -> bool {
        self.fixed == 0 && self.per_row == 0 && self.per_input_byte == 0
    }

    /// A reader-sized bound needs no payload walk or per-batch reservation.
    pub(super) fn row_bound(self, rows: usize) -> Option<usize> {
        (self.per_input_byte == 0)
            .then(|| self.per_row.checked_mul(rows)?.checked_add(self.fixed))
            .flatten()
    }

    pub(super) fn bytes(self, batch: &RecordBatch) -> Result<usize> {
        let input_bytes = if self.per_input_byte == 0 {
            0
        } else {
            batch.get_array_memory_size()
        };
        self.bytes_for(batch.num_rows(), input_bytes)
    }

    pub(super) fn bytes_for(self, rows: usize, input_bytes: usize) -> Result<usize> {
        self.per_row
            .checked_mul(rows)
            .and_then(|n| n.checked_add(self.fixed))
            .and_then(|n| {
                self.per_input_byte
                    .checked_mul(input_bytes)
                    .and_then(|input| n.checked_add(input))
            })
            .ok_or_else(|| "resource-limit: lane predicate workspace overflow".into())
    }
}

pub(super) struct PredicateMemory {
    pub cloned_bytes: usize,
    pub scratch: Scratch,
    // Total UTF-8 payload of a text result, used to bound its parent's workspace.
    text: Scratch,
}

impl PredicateMemory {
    pub(super) fn for_expression(expression: &Expr) -> Option<Self> {
        let mut memory = Self {
            cloned_bytes: std::mem::size_of::<Expr>(),
            scratch: Scratch::default(),
            text: Scratch::default(),
        };
        match &expression.kind {
            Kind::Column(_) => {
                if expression.ty == Type::Text {
                    memory.text.per_input_byte = 1;
                }
            }
            Kind::Literal(literal) => {
                let bytes = match literal {
                    Literal::Text(text) => {
                        memory.text.per_row = text.len();
                        memory.scratch = memory.text.scale(2)?.add(Scratch {
                            // StringArray::from_iter starts with 1024 bytes.
                            fixed: 1024,
                            ..Scratch::default()
                        })?;
                        text.len()
                    }
                    Literal::Binary(bytes) => bytes.len(),
                    _ => 0,
                };
                memory.cloned_bytes = memory.cloned_bytes.checked_add(bytes)?;
            }
            Kind::Not(child) | Kind::Neg(child) | Kind::IsNull(child, _) => {
                memory.include(&Self::for_expression(child)?)?;
            }
            Kind::Substring(child, _, length) => {
                let child = Self::for_expression(child)?;
                memory.include(&child)?;
                // Each selected Unicode scalar uses at most four UTF-8 bytes.
                // A bounded slice therefore needs no whole-input payload bound.
                // Keep the child's bound when its literal is even shorter, or
                // the character limit is too large to multiply safely.
                memory.text = match length.checked_mul(4) {
                    Some(bytes) if child.text.per_input_byte == 0 => Scratch {
                        per_row: bytes.min(child.text.per_row),
                        ..Scratch::default()
                    },
                    Some(bytes) => Scratch {
                        per_row: bytes,
                        ..Scratch::default()
                    },
                    None => child.text,
                };
                // Prepay geometric byte-buffer growth; Expr::eval already
                // funds offsets, validity, descriptors and numeric workspace.
                memory.scratch = memory.scratch.add(memory.text.scale(2)?)?.add(Scratch {
                    fixed: 1024,
                    ..Scratch::default()
                })?;
            }
            Kind::Binary(_, left, right) | Kind::Like(left, right, _) => {
                let a = Self::for_expression(left)?;
                let b = Self::for_expression(right)?;
                memory.include(&a)?;
                if matches!(expression.kind, Kind::Like(..))
                    && matches!(right.kind, Kind::Literal(Literal::Text(_)))
                {
                    // Literal LIKE patterns are borrowed, never broadcast.
                    memory.cloned_bytes = memory.cloned_bytes.checked_add(b.cloned_bytes)?;
                } else {
                    memory.include(&b)?;
                }
                if matches!(expression.kind, Kind::Like(..)) {
                    let matching = if let Kind::Literal(Literal::Text(pattern)) = &right.kind {
                        let parts = pattern
                            .bytes()
                            .filter(|b| *b == b'%')
                            .count()
                            .checked_add(1)?;
                        let entry = std::mem::size_of::<&str>()
                            .checked_add(std::mem::size_of::<memchr::memmem::Finder<'_>>())?;
                        // At most one borrowed finder per part; allow doubled
                        // Vec capacity. Finder borrows the funded literal.
                        let mut matching = Scratch {
                            fixed: parts.checked_mul(entry)?.checked_mul(2)?.checked_add(256)?,
                            ..Scratch::default()
                        };
                        if pattern.contains('_') {
                            matching = matching.add(a.text.scale(8)?)?.add(Scratch {
                                fixed: pattern.len().checked_mul(8)?.checked_add(64)?,
                                ..Scratch::default()
                            })?;
                        }
                        matching
                    } else {
                        // Dynamic LIKE uses two temporary Vec<char>s per row.
                        // UTF-8 bytes bound character counts; 8 covers four-byte
                        // chars and doubled capacity, with slack for empty Vecs.
                        a.text.add(b.text)?.scale(8)?.add(Scratch {
                            fixed: 64,
                            ..Scratch::default()
                        })?
                    };
                    memory.scratch = memory.scratch.add(matching)?;
                }
            }
            // Future totality admissions must supply their own allocation bound.
            _ => return None,
        }
        Some(memory)
    }

    fn include(&mut self, child: &Self) -> Option<()> {
        self.cloned_bytes = self.cloned_bytes.checked_add(child.cloned_bytes)?;
        self.scratch = self.scratch.add(child.scratch)?;
        Some(())
    }
}
