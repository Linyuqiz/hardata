include!("jobs/queries.rs");
include!("jobs/cleanup.rs");
include!("jobs/mapping.rs");

#[cfg(test)]
mod tests {
    include!("jobs/tests.rs");
}
