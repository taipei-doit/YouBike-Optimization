# Python GIS / bridge / parity / preprocess batch image
FROM continuumio/miniconda3:23.10.0-1

WORKDIR /work

COPY environment.yml ./
RUN conda env create -f environment.yml \
    && conda clean -afy

# Activate env for subsequent RUNs and default shell
SHELL ["conda", "run", "-n", "youbike_potential_demand", "/bin/bash", "-c"]

COPY . ./

ENV PATH=/opt/conda/envs/youbike_potential_demand/bin:$PATH
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "youbike_potential_demand"]
CMD ["python", "-c", "print('python-batch ready')"]
