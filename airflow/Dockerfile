ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.8.3-python3.11
FROM ${AIRFLOW_IMAGE_NAME}



COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ENV CLOUD_SDK_VERSION=460.0.0
# ENV PATH="/google-cloud-sdk/bin:${PATH}"


USER root

RUN apt-get update && \
    apt-get install -y wget unzip && \
    apt-get install -y gnupg && \
    apt-get install -y curl && \
    apt-get install -y python3-dev && \
    apt-get install -y apt-transport-https && \
    apt-get install -y ca-certificates && \
    apt-get install -y software-properties-common && \
    rm -rf /var/lib/apt/lists/*

# RUN curl https://sdk.cloud.google.com | bash
# ENV PATH $PATH:/root/google-cloud-sdk/bin

# RUN gcloud version
# RUN gcloud components install gsutil

# RUN chmod +x /root/google-cloud-sdk/bin/gsutil

# # Downloading gcloud package
# RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# # Installing the package
# RUN mkdir -p /usr/local/gcloud \
#   && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
#   && /usr/local/gcloud/google-cloud-sdk/install.sh

# # Adding the package path to local
# ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


# # Add Google Cloud SDK distribution URI as a package source
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# # Import the Google Cloud public key
# RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# # Update the package list and install the Cloud SDK
# RUN apt-get update && apt-get install -y google-cloud-sdk

# # Set the environment variable
# ENV GOOGLE_APPLICATION_CREDENTIALS=/.google/credentials/google_credentials.json

# RUN gcloud config set project inbound-ship-412204

# RUN gsutil config -n

# ARG VERSION=v4.9.6
# ARG BINARY=yq_linux_386
# RUN wget https://github.com/mikefarah/yq/releases/download/${VERSION}/${BINARY} -O /usr/bin/yq \ 
#     && chmod +x /usr/bin/yq

USER airflow