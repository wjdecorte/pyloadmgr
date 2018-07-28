{
    "$schema" : "http://json-schema.org/draft-04/schema#",
    "id" : "/",
    "type" : "object",
    "properties" : {
        "job_step" : {
            "id" : "job_step",
            "type" : "string"
        },
        "job_step_wait_step" : {
            "id" : "job_step_wait_step",
            "type" : "string"
        },
        "job_step_alt_wait_step" : {
            "id" : "job_step_alt_wait_step",
            "type" : "string"
        },
        "job_step_description" : {
            "id" : "job_step_description",
            "type" : "string"
        },
        "job_step_function" : {
            "id" : "job_step_function",
            "type" : "string"
        },
        "job_step_command" : {
            "id" : "job_step_command",
            "type" : "string"
        },
        "job_step_args" : {
            "id" : "job_step_args",
            "type" : "string"
        },
        "job_step_ignore_error" : {
            "id" : "job_step_ignore_error",
            "type" : "boolean"
        },
        "job_step_capture_output" : {
            "id" : "job_step_capture_output",
            "type" : "boolean"
        }
    },
    "additionalProperties" : false,
    "required" : [
        "job_step",
        "job_step_description",
        "job_step_function",
        "job_step_command",
        "job_step_args",
        "job_step_ignore_error",
        "job_step_capture_output"
    ]
}
