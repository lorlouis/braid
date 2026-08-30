#![no_main]

//! The datagram's outermost decoder, and the only one that allocates from a
//! length it is not told: deflate carries no size on the wire, so the ceiling
//! has to be applied to the slice the decompressor is handed instead. Anything
//! at all may be handed to `unpack` without a panic, and whatever `pack`
//! produced comes back exactly.

use braid_proto::wire::{pack, unpack};
use libfuzzer_sys::fuzz_target;

/// What a real caller states: the largest frame this direction accepts.
const LIMIT: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    let mut out = Vec::new();
    let _ = unpack(data, LIMIT, &mut out);
    assert!(
        out.len() <= LIMIT,
        "unpack reserved {} bytes against a {LIMIT}-byte ceiling",
        out.len()
    );

    // Including the case the tag exists for: deflate would have grown the
    // payload and the stored form is what went out.
    if data.len() <= LIMIT {
        let mut packed = Vec::new();
        pack(data, &mut packed);
        assert!(
            packed.len() <= data.len() + 1,
            "packing {} bytes produced {}",
            data.len(),
            packed.len()
        );
        let mut scratch = Vec::new();
        let back = unpack(&packed, LIMIT, &mut scratch).expect("a payload this side packed");
        assert_eq!(back, data, "the codec is not its own inverse");
    }

    // The answer is borrowed from the datagram or from the scratch, and the
    // scratch is caller-owned and reused per datagram: a decoder that appends
    // rather than replaces, or that answers out of whatever the last frame
    // left behind, corrupts under load and passes every single-shot test.
    let mut warm = vec![0xAB; 4096];
    let warm_answer = unpack(data, LIMIT, &mut warm).ok().map(<[u8]>::to_vec);
    let mut cold = Vec::new();
    let cold_answer = unpack(data, LIMIT, &mut cold).ok().map(<[u8]>::to_vec);
    assert_eq!(
        warm_answer, cold_answer,
        "unpack depends on the buffer it was given"
    );
});
