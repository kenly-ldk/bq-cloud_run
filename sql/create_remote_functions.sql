-- Standard Multi-Parameter Detokenization Function
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_fpe_multi(val STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
    endpoint='https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
    max_batching_rows=100
);

-- Standard Multi-Parameter Tokenization Function (Real-Time Search)
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_tokenize_fpe_multi(val STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
    endpoint='https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
    user_defined_context=[('mode', 'tokenize')],
    max_batching_rows=100
);

-- Batched Functions for Scale Testing
-- Batch Size 100
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b100(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 100
);

-- Batch Size 1000
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b1000(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 1000
);

-- Batch Size 5000
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b5000(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 5000
);

-- Batch Size 10000
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b10000(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 10000
);

-- Batch Size 50000
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b50000(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 50000
);

-- Batch Size 100000
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_detokenize_b100000(value STRING, data_element STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
  endpoint = 'https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
  max_batching_rows = 100000
);

-- Noop Function for Pure Network Transit Benchmarking
CREATE OR REPLACE FUNCTION pii_transform_demo.pii_noop(val STRING)
RETURNS STRING
REMOTE WITH CONNECTION `<YOUR_PROJECT_ID>.<YOUR_REGION>.pii_transform_conn`
OPTIONS (
    endpoint='https://pii-transform-service-protegrity-<YOUR_PROJECT_NUMBER>.<YOUR_REGION>.run.app',
    max_batching_rows=100,
    user_defined_context=[('mode', 'noop')]
);
