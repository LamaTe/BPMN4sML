{
  "StartAt": "SourceDataActivity_0aovg1y",
  "States": {
    "SourceDataActivity_0aovg1y": {
      "Type": "Task",
      "Resource": "SourceData_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "Next": "EngineerFeatsActivity_1abs81x"
    },
    "EngineerFeatsActivity_1abs81x": {
      "Type": "Task",
      "Resource": "EngineerFeats_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "Next": "SplitDataActivity_0futsc2"
    },
    "SplitDataActivity_0futsc2": {
      "Type": "Task",
      "Resource": "SplitData_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "Next": "TuneModelActivity_0ifrkn2"
    },
    "TuneModelActivity_0ifrkn2": {
      "Type": "Task",
      "Resource": "TuneModel_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "Next": "VerifyModelActivity_18kjven"
    },
    "VerifyModelActivity_18kjven": {
      "Type": "Task",
      "Resource": "VerifyModel_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "Next": "DeployModelActivity_1h0f6f0"
    },
    "DeployModelActivity_1h0f6f0": {
      "Type": "Task",
      "Resource": "DeployModel_ARN",
      "TimeoutSeconds": 300000, 
      "Retry": [
        {
          "ErrorEquals": [ "States.ALL" ],
          "MaxAttempts": 0
        }
      ],
      "End": true
    }
  }
}