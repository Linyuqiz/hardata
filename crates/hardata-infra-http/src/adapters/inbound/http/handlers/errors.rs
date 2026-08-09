fn map_finalize_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::InvalidConfig(message) if message.contains("overlaps active job") => {
            StatusCode::CONFLICT
        }
        HarDataError::JobNotFound(_) => StatusCode::NOT_FOUND,
        HarDataError::InvalidConfig(_)
        | HarDataError::InvalidProtocol(_)
        | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message) if message.contains("not found") => StatusCode::NOT_FOUND,
        HarDataError::Unknown(message) if message.contains("still shutting down") => {
            StatusCode::CONFLICT
        }
        HarDataError::Unknown(message)
            if message.contains("already has active final transfer")
                || message.contains("already active with status") =>
        {
            StatusCode::CONFLICT
        }
        HarDataError::Unknown(message)
            if message.contains("not a sync job") || message.contains("cannot be finalized") =>
        {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn map_create_job_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::InvalidConfig(message)
            if message.contains("already used by active job")
                || message.contains("overlaps active job") =>
        {
            StatusCode::CONFLICT
        }
        HarDataError::InvalidConfig(_) | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message)
            if message.contains("still shutting down")
                || message.contains("already active with status") =>
        {
            StatusCode::CONFLICT
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn map_cancel_error_status(error: &HarDataError) -> StatusCode {
    match error {
        HarDataError::JobNotFound(_) => StatusCode::NOT_FOUND,
        HarDataError::InvalidConfig(_) | HarDataError::FileOperation(_) => StatusCode::BAD_REQUEST,
        HarDataError::Unknown(message) if message.contains("not found") => StatusCode::NOT_FOUND,
        HarDataError::Unknown(message)
            if message.contains("already finished") || message.contains("already failed") =>
        {
            StatusCode::CONFLICT
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
