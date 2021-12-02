use crate::error::Result;

#[inline(always)]
pub fn and(l: Option<bool>, r: Option<bool>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(lb), Some(lr)) => Ok(Some(lb & lr)),
        (Some(true), None) => Ok(None),
        (None, Some(true)) => Ok(None),
        (Some(false), None) => Ok(Some(false)),
        (None, Some(false)) => Ok(Some(false)),
        (None, None) => Ok(None),
    }
}

#[inline(always)]
pub fn or(l: Option<bool>, r: Option<bool>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(lb), Some(lr)) => Ok(Some(lb | lr)),
        (Some(true), None) => Ok(Some(true)),
        (None, Some(true)) => Ok(Some(true)),
        (Some(false), None) => Ok(None),
        (None, Some(false)) => Ok(None),
        (None, None) => Ok(None),
    }
}

#[inline(always)]
pub fn not(l: Option<bool>) -> Result<Option<bool>> {
    Ok(l.map(|v| !v))
}

#[cfg(test)]
mod tests {
    use crate::vector_op::conjunction::{and, or};

    #[test]
    fn test_and() {
        assert_eq!(Some(true), and(Some(true), Some(true)).unwrap());
        assert_eq!(Some(false), and(Some(true), Some(false)).unwrap());
        assert_eq!(Some(false), and(Some(false), Some(false)).unwrap());
        assert_eq!(None, and(Some(true), None).unwrap());
        assert_eq!(Some(false), and(Some(false), None).unwrap());
        assert_eq!(None, and(None, None).unwrap());
    }

    #[test]
    fn test_or() {
        assert_eq!(Some(true), or(Some(true), Some(true)).unwrap());
        assert_eq!(Some(true), or(Some(true), Some(false)).unwrap());
        assert_eq!(Some(false), or(Some(false), Some(false)).unwrap());
        assert_eq!(Some(true), or(Some(true), None).unwrap());
        assert_eq!(None, or(Some(false), None).unwrap());
        assert_eq!(None, or(None, None).unwrap());
    }
}
