use itertools::Itertools;

/// This function compresses sequential repeated data in a vector. The compression result contains
/// two vectors, one for the last indices of sequential repeated elements, and another for the
/// repeated data. For example, [14, 14, 14, 27, 27] will be compressed to [2, 4], [14, 27].
pub fn compress_data<T>(original_data: &[T]) -> (Vec<u64>, Vec<T>)
where
    T: PartialEq + Copy,
{
    let mut original_indices = Vec::new();
    let mut data = Vec::new();

    for i in 1..original_data.len() {
        if original_data[i - 1] != original_data[i] {
            original_indices.push(i as u64 - 1);
            data.push(original_data[i - 1]);
        }
    }

    if let Some(&last) = original_data.last() {
        original_indices.push(original_data.len() as u64 - 1);
        data.push(last);
    }

    (original_indices, data)
}

/// Works in a reversed way as `compress_data`.
pub fn decompress_data<T>(original_indices: Vec<u64>, data: Vec<T>) -> Vec<T>
where
    T: Clone,
{
    match original_indices.last() {
        Some(last_idx) => {
            let mut original_data = Vec::with_capacity(*last_idx as usize + 1);
            original_indices
                .into_iter()
                .zip_eq(data)
                .for_each(|(idx, x)| {
                    original_data.resize(idx as usize + 1, x);
                });
            original_data
        }
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{compress_data, decompress_data};

    #[test]
    fn test_compress() {
        // Simple
        let original_data = [3u32, 3, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 8, 8, 9];
        let (compressed_original_indices, compressed_data) = compress_data(&original_data);
        let expect_original_indices = Vec::from([4u64, 6, 8, 9, 10, 13, 14]);
        let expect_data = Vec::from([3u32, 4, 5, 6, 7, 8, 9]);
        assert_eq!(compressed_original_indices, expect_original_indices);
        assert_eq!(compressed_data, expect_data);
        let decompressed_data = decompress_data(compressed_original_indices, compressed_data);
        assert_eq!(decompressed_data, original_data);

        // Complex
        let mut long_original_data = Vec::new();
        long_original_data.resize(512, 1);
        long_original_data.resize(1024, 2);
        long_original_data.resize(1536, 3);
        long_original_data.resize(2048, 4);
        long_original_data[0] = 5;
        long_original_data[2046] = 5;
        let (compressed_original_indices, compressed_data) = compress_data(&long_original_data);
        let expect_original_indices = Vec::from([0u64, 511, 1023, 1535, 2045, 2046, 2047]);
        let expect_data = Vec::from([5u32, 1, 2, 3, 4, 5, 4]);
        assert_eq!(compressed_original_indices, expect_original_indices);
        assert_eq!(compressed_data, expect_data);
        let decompressed_data = decompress_data(compressed_original_indices, compressed_data);
        assert_eq!(decompressed_data, long_original_data);

        // Empty
        let (compressed_original_indices, compressed_data) = compress_data::<u8>(&[]);
        assert!(compressed_original_indices.is_empty());
        assert!(compressed_data.is_empty());
        let decompressed_data = decompress_data(compressed_original_indices, compressed_data);
        assert!(decompressed_data.is_empty());
    }
}
