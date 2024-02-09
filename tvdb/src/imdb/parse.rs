use anyhow::{Context, Result};

pub fn remove_na(s: &str) -> Option<&str> {
    match s {
        "\\N" => None,
        _ => Some(s),
    }
}

pub fn parse_int(s: &str) -> Result<Option<i32>> {
    match s {
        "\\N" => Ok(None),
        _ => Ok(Some(
            s.parse::<i32>()
                .context(format!("Failed to parse int: {}", s))?,
        )),
    }
}

pub fn parse_float(s: &str) -> Result<Option<f32>> {
    match s {
        "\\N" => Ok(None),
        _ => Ok(Some(
            s.parse::<f32>()
                .context(format!("Failed to parse float: {}", s))?,
        )),
    }
}
