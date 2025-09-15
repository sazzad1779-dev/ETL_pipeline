# Start from official Weaviate image
FROM cr.weaviate.io/semitechnologies/weaviate:latest

# Set environment variables
ENV QUERY_DEFAULTS_LIMIT=40 \
    ENABLE_MODULES="backup-s3,text2vec-openai" \
    ENABLE_API_BASED_MODULES="true" \
    CLUSTER_HOSTNAME="node1" \
    PROMETHEUS_MONITORING_ENABLED="true" \
    PROMETHEUS_MONITORING_PORT=2112 \
    AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED="false" \
    AUTHENTICATION_APIKEY_ENABLED="true" \
    AUTHENTICATION_APIKEY_ALLOWED_KEYS="jbc_admin,jbc_dev" \
    AUTHENTICATION_APIKEY_USERS="admin,user" \
    BACKUP_S3_ENDPOINT="s3.amazonaws.com" \
    BACKUP_S3_BUCKET="vectordb.backup" \
    BACKUP_S3_PATH="weaviate/prod" \
    BACKUP_S3_REGION="ap-northeast-1" \
    PERSISTENCE_DATA_PATH="/var/lib/weaviate" \
    DISABLE_TELEMETRY="true"

# AWS credentials can be passed at runtime (best practice, not baked into image)
ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=yyy

# Expose ports
EXPOSE 8080 50051 2112

# Set default command (same as your compose command)
CMD ["--host", "0.0.0.0", "--port", "8080", "--scheme", "http"]
