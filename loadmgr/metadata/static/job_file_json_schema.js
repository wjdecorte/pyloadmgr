{
    "$schema" : "http://json-schema.org/draft-04/schema#",
    "id" : "/",
    "type" : "object",
    "properties" : {
        "steps" : {
            "id" : "steps",
            "type" : "array",
            "items" : {
                "id" : "step",
                "type" : "object",
                "properties" : {
                    "skip_step" : {
                        "id" : "skip_step",
                        "type" : "boolean"
                    },
                    "job_step" : {
                        "id" : "job_step",
                        "type" : "integer"
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
                        "type" : "array",
                        "minItems" : 1,
                        "items" : {
                            "id" : "command",
                            "type" : "string"
                        }
                    },
                    "job_step_args" : {
                        "id" : "job_step_args",
                        "type" : "object",
                        "properties" : {
                            "cli_args" : {
                                "id" : "cli_args",
                                "type" : "array",
                                "items" : {
                                    "id" : "arg",
                                    "type" : "string"
                                }
                            },
                            "start_date" : {
                                "id" : "start_date",
                                "type" : "string"
                            },
                            "end_date" : {
                                "id" : "end_date",
                                "type" : "string"
                            },
                            "load_strategy" : {
                                "id" : "load_strategy",
                                "type" : "string"
                            },
                            "date_format" : {
                                "id" : "date_format",
                                "type" : "string"
                            }
                        },
                        "additionalProperties" : true
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
                    "skip_step",
                    "job_step",
                    "job_step_description",
                    "job_step_function",
                    "job_step_command",
                    "job_step_args",
                    "job_step_ignore_error",
                    "job_step_capture_output"
                ]
            },
            "minItems" : 1
        }
    },
    "additionalProperties" : false,
    "required" : [
        "steps"
    ]
}
