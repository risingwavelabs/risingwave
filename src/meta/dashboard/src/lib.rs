pub fn add(left: usize, right: usize) -> usize {
    left + right
}

const A: &str = env!("OUT_DIR");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
