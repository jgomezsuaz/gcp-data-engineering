# GCP Data Engineering Practices

Ejemplos de uso de varias tecnologías de Google Cloud orientadas a la ingeniería de datos

## Dataflow

Para continuar, debes:

1. Crear un Bucket de Cloud Storage y colocarle de nombre el ID del proyecto
2. Utilizar los siguientes comandos en Cloud Shell

```bash
  sudo apt-get update && sudo apt-get install -y python3-venv
```

```bash
  gcloud services enable dataflow.googleapis.com
```

### 1 Basic Example

```bash
  cd dataflow/1_BasicExample/
```

```bash
  pip install -r requirements.txt 
```

#### Batch

```bash
  export PROJECT_ID=$(gcloud config get-value project)
```

```bash
  python3 data_collector.py \
    --project=${PROJECT_ID}
```

```bash
python3 my_pipeline_batch.py \
  --project=${PROJECT_ID} \
  --region=us-central1 \
  --stagingLocation=gs://$PROJECT_ID/staging/ \
  --tempLocation=gs://$PROJECT_ID/temp/ \
  --runner=DataflowRunner
```

#### Streaming

el proceso está fallando, pendiente por ser revisada la causa

```bash
  python3 sensor_simulator.py \
    --project=${PROJECT_ID} \
    --topic=dataflow-sensors-topic \
    --dataLocation=iot_telemetry_data.csv \
    --dataPerMinute=60
```

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export BUCKET=gs://${PROJECT_ID}
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner
export PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/dataflow-sensors-topic
export WINDOW_DURATION=60
export AGGREGATE_TABLE_NAME=${PROJECT_ID}:logs.windowed_avg
export RAW_TABLE_NAME=${PROJECT_ID}:logs.raw_sensors
python3 my_pipeline_streaming.py \
--project=${PROJECT_ID} \
--region=${REGION} \
--staging_location=${PIPELINE_FOLDER}/staging \
--temp_location=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER} \
--input_topic=${PUBSUB_TOPIC} \
--window_duration=${WINDOW_DURATION} \
--agg_table_name=${AGGREGATE_TABLE_NAME} \
--raw_table_name=${RAW_TABLE_NAME} 
```
