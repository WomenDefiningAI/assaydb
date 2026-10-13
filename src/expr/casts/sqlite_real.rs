//! TEXT/BLOB REAL conversion matching the SQLite bundled with frozen SLT.
//!
//! Port of public-domain sqlite3AtoF, sqlite3Fp10Convert2 and powerOfTen
//! from SQLite 3.54.0 source d232720b7932c1cebe4cf5b1a1e9096889e1807844351390149dfeb2ea7fc304.
//! See research/SQL-CAST-REAL-REFERENCE.json for artifact hashes and license.
//! In particular, discarded mantissa digits MUST NOT affect binary rounding.
//! Integer arithmetic gives the same results on every supported CPU; no FMA,
//! host extended precision, libc conversion, or runtime SQLite dependency.

const BASE: [u64; 27] = [
    0x8000000000000000,
    0xa000000000000000,
    0xc800000000000000,
    0xfa00000000000000,
    0x9c40000000000000,
    0xc350000000000000,
    0xf424000000000000,
    0x9896800000000000,
    0xbebc200000000000,
    0xee6b280000000000,
    0x9502f90000000000,
    0xba43b74000000000,
    0xe8d4a51000000000,
    0x9184e72a00000000,
    0xb5e620f480000000,
    0xe35fa931a0000000,
    0x8e1bc9bf04000000,
    0xb1a2bc2ec5000000,
    0xde0b6b3a76400000,
    0x8ac7230489e80000,
    0xad78ebc5ac620000,
    0xd8d726b7177a8000,
    0x878678326eac9000,
    0xa968163f0a57b400,
    0xd3c21bcecceda100,
    0x84595161401484a0,
    0xa56fa5b99019a5c8,
];

const SCALE: [u64; 26] = [
    0x8049a4ac0c5811ae,
    0xcf42894a5dce35ea,
    0xa76c582338ed2621,
    0x873e4f75e2224e68,
    0xda7f5bf590966848,
    0xb080392cc4349dec,
    0x8e938662882af53e,
    0xe65829b3046b0afa,
    0xba121a4650e4ddeb,
    0x964e858c91ba2655,
    0xf2d56790ab41c2a2,
    0xc428d05aa4751e4c,
    0x9e74d1b791e07e48,
    0xcccccccccccccccc,
    0xcecb8f27f4200f3a,
    0xa70c3c40a64e6c51,
    0x86f0ac99b4e8dafd,
    0xda01ee641a708de9,
    0xb01ae745b101e9e4,
    0x8e41ade9fbebc27d,
    0xe5d3ef282a242e81,
    0xb9a74a0637ce2ee1,
    0x95f83d0a1fb69cd9,
    0xf24a01a73cf2dccf,
    0xc3b8358109e84f07,
    0x9e19db92b4e31ba9,
];

const SCALE_LO: [u32; 26] = [
    0x205b896d, 0x52064cad, 0xaf2af2b8, 0x5a7744a7, 0xaf39a475, 0xbd8d794e, 0x547eb47b, 0x0cb4a5a3,
    0x92f34d62, 0x3a6a07f9, 0xfae27299, 0xaa97e14c, 0x775ea265, 0xcccccccc, 0x00000000, 0x999090b6,
    0x69a028bb, 0xe80e6f48, 0x5ec05dd0, 0x14588f14, 0x8f1668c9, 0x6d953e2c, 0x4abdaf10, 0xbc633b39,
    0x0a862f81, 0x6c07a2c2,
];

fn power_of_ten(p: i32) -> (u64, u32) {
    debug_assert!((-348..=347).contains(&p));
    if p == -1 {
        return (SCALE[13], SCALE_LO[13]);
    }
    if (0..27).contains(&p) {
        return (BASE[p as usize], 0);
    }
    let g = p.div_euclid(27);
    let n = p.rem_euclid(27) as usize;
    let index = (g + 13) as usize;
    if n == 0 {
        return (SCALE[index], SCALE_LO[index]);
    }
    // Upper 96 bits of a 96-bit by 64-bit product (sqlite3Multiply160).
    let r = u128::from(SCALE[index]) * u128::from(BASE[n])
        + ((u128::from(SCALE_LO[index]) * u128::from(BASE[n])) >> 32);
    let mut hi = (r >> 64) as u64;
    let mut lo = (r >> 32) as u32;
    if hi & (1 << 63) == 0 {
        hi = (hi << 1) | u64::from(lo >> 31);
        lo = (lo << 1) | 1;
    }
    (hi, lo)
}

fn decimal_to_binary(d: u64, p: i64) -> f64 {
    if p < -348 {
        return 0.0;
    }
    if p > 347 {
        return f64::INFINITY;
    }
    debug_assert_ne!(d, 0);
    let p = p as i32;
    let b = 64 - d.leading_zeros() as i32;
    let lp = (p * 108853) >> 15;
    let mut e = 53 - b - lp;
    if e > 1074 {
        if e >= 1130 {
            return 0.0;
        }
        e = 1074;
    }
    let s = -(e - (64 - b) + lp + 3);
    debug_assert!((0..64).contains(&s));
    let (mut pwr_hi, mut pwr_lo) = power_of_ten(p);
    if pwr_lo != 0 {
        pwr_hi = pwr_hi.wrapping_add(1);
        pwr_lo = !pwr_lo;
    }
    let x = d << (64 - b);
    let product = u128::from(x) * u128::from(pwr_hi);
    let mut hi = (product >> 64) as u64;
    let mid1 = (product >> 32) as u32;
    let mut sticky = 1;
    if hi & ((1u64 << s) - 1) == 0 {
        let mid2 = ((u128::from(x) * (u128::from(pwr_lo) << 32)) >> 96) as u32;
        sticky = u64::from(mid1.wrapping_sub(mid2) > 1);
        hi = hi.wrapping_sub(u64::from(mid1 < mid2));
    }
    let mut u = (hi >> s) | sticky;
    if u >= (1u64 << 55) - 2 {
        u = (u >> 1) | (u & 1);
        e -= 1;
    }
    let mut m = (u + 1 + ((u >> 2) & 1)) >> 2;
    if e <= -972 {
        return f64::INFINITY;
    }
    if m & (1 << 52) != 0 {
        m = (m & !(1 << 52)) | (((1075 - e) as u64) << 52);
    }
    f64::from_bits(m)
}

/// SQLite's maximal numeric prefix, with its bounded decimal significand.
/// This returns the value even when trailing bytes make the input nonnumeric
/// as a whole, which is exactly what CAST requests of sqlite3AtoF.
/// The byte slice may contain NUL or invalid UTF-8 (BLOB inputs).
pub(super) fn parse(bytes: &[u8]) -> f64 {
    const LIMIT: u64 = (u64::MAX - 9) / 10;
    let mut at = super::leading(bytes);
    let negative = bytes.get(at) == Some(&b'-');
    if matches!(bytes.get(at), Some(b'+' | b'-')) {
        at += 1;
    }
    let mut mantissa = 0u64;
    // i64 avoids exponent-counter overflow on the largest Arrow byte slices.
    let mut exponent = 0i64;
    let mut digit_seen = false;
    while let Some(&digit) = bytes.get(at).filter(|b| b.is_ascii_digit()) {
        digit_seen = true;
        mantissa = mantissa * 10 + u64::from(digit - b'0');
        at += 1;
        if mantissa >= LIMIT {
            while bytes.get(at).is_some_and(u8::is_ascii_digit) {
                exponent += 1;
                at += 1;
            }
            break;
        }
    }
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        while let Some(&digit) = bytes.get(at).filter(|b| b.is_ascii_digit()) {
            digit_seen = true;
            if mantissa < LIMIT {
                mantissa = mantissa * 10 + u64::from(digit - b'0');
                exponent -= 1;
            }
            at += 1;
        }
    }
    if !digit_seen {
        return 0.0;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        let negative_exponent = bytes.get(at) == Some(&b'-');
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        let mut explicit = 0i64;
        while let Some(&digit) = bytes.get(at).filter(|b| b.is_ascii_digit()) {
            explicit = if explicit < 10000 {
                explicit * 10 + i64::from(digit - b'0')
            } else {
                10000
            };
            at += 1;
        }
        exponent += if negative_exponent {
            -explicit
        } else {
            explicit
        };
    }
    let value = if mantissa == 0 {
        0.0
    } else {
        decimal_to_binary(mantissa, exponent)
    };
    if negative { -value } else { value }
}

#[cfg(test)]
mod tests {
    #[test]
    fn matches_pinned_sqlite_exponent_mantissa_and_seeded_long_decimal_oracle() {
        // SQL-generated bits from the pinned amalgamation, not Rust parsing or
        // a second copy of the conversion algorithm. This includes infinities
        // internally; the public CAST boundary still rejects nonfinite values.
        let fixture = include_str!("../../../tests/fixtures/sqlite-real-conversion.tsv");
        let mut headers = fixture.lines();
        assert_eq!(
            headers.next(),
            Some(
                "# SQLite 3.54.0: 2026-10-07 10:41:59 d232720b7932c1cebe4cf5b1a1e9096889e1807844351390149dfeb2ea7fc304"
            )
        );
        assert_eq!(
            headers.next(),
            Some(
                "# Amalgamation SHA256 b739c477092f587417d402fe227eb13c0a83f7534e87082f4a1b80d429a57931"
            )
        );
        let mut count = 0;
        for line in fixture.lines().filter(|line| !line.starts_with('#')) {
            let (bits, text) = line.split_once('\t').unwrap();
            let expected = u64::from_str_radix(bits, 16).unwrap();
            assert_eq!(
                super::parse(text.as_bytes()).to_bits(),
                expected,
                "{text:?}"
            );
            count += 1;
        }
        assert_eq!(count, 7584);
    }
}
