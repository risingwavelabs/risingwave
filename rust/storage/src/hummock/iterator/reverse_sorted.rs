use super::variants::BACKWARD;
use crate::hummock::iterator::sorted_inner::SortedIteratorInner;

pub type ReverseSortedIterator = SortedIteratorInner<BACKWARD>;

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of, test_value_of, TestIteratorBuilder, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::{BoxedHummockIterator, HummockIterator};

    #[tokio::test]
    async fn test_reverse_sorted_basic() {
        let base_key_value = usize::MAX - 10;
        let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
            .map(|iter_id| {
                TestIteratorBuilder::default()
                    .id(0)
                    .map_key(move |id, x| {
                        iterator_test_key_of(id, base_key_value - x * 3 + (3 - iter_id as usize))
                    })
                    .map_value(move |id, x| {
                        test_value_of(id, base_key_value - x * 3 + (3 - iter_id as usize))
                    })
                    .finish()
            })
            .unzip();

        let iters: Vec<BoxedHummockIterator> = iters
            .into_iter()
            .map(|x| Box::new(x) as BoxedHummockIterator)
            .collect_vec();

        let mut mi = ReverseSortedIterator::new(iters);
        let mut i = 0;
        mi.rewind().await.unwrap();
        while mi.is_valid() {
            let key = mi.key();
            let val = mi.value();
            validators[i % 3].assert_key(i / 3, key);
            validators[i % 3].assert_hummock_value(i / 3, val);
            i += 1;
            mi.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!mi.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT);
    }
}
