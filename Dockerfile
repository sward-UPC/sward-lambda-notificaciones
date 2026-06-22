FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}/

RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

COPY handler.py ${LAMBDA_TASK_ROOT}/
COPY lib/ ${LAMBDA_TASK_ROOT}/lib/

CMD ["handler.lambda_handler"]
