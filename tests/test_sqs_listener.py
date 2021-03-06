from data_egress import sqs_listener
import json
import pytest
from data_egress.sqs_listener import S3PrefixAndDynamoRecord
from moto import mock_s3, mock_dynamodb2, mock_sqs, mock_sts
import boto3
import argparse
import logging
import zlib
import datetime

from Crypto.Cipher import AES
from Crypto.Util import Counter
import base64


SERVICE_S3 = "s3"

IV_BASE64 = "BDva/T7HssDYMtyLfn/afw=="
IV_INT = 5627699127241421480342634160438893183
SERVICE_IAM = "iam"
DESTINATION_BUCKET_ROLE = "destination_bucket_role"
PLAIN_TEXT_KEY = "UBkbtizlrjYs5kZch3CwCg=="
GZIP_VALUE = "gzip"
ROLE_ARN_VALUE = "arn:aws:iam::123456789012:role/destination_bucket_role"
SERVICE_DYNAMODB = "dynamodb"
TEST_SQS_QUEUE = "test-sqs-queue"
TEST_DATA = "test_data"
SERVICE_SQS = "sqs"
BODY = "Body"
MESSAGES = "Messages"
ATTRIBUTES = "Attributes"
NUMBER_OF_MESSAGES = "ApproximateNumberOfMessages"
KEY_ROLE_ARN = "role_arn"
KEY_COMPRESSION_FMT = "compression_fmt"
KEY_COMPRESS = "compress"
KEY_RECIPIENT_NAME = "recipient_name"
KEY_TRANSFER_TYPE = "transfer_type"
KEY_DESTINATION_BUCKET = "destination_bucket"
KEY_SOURCE_BUCKET = "source_bucket"
KEY_DESTINATION_PREFIX = "destination_prefix"
KEY_DECRYPT = "decrypt"
DESTINATION_PREFIX_VALUE = "output/"
KEY_SOURCE_PREFIX = "source_prefix"
KEY_PIPELINE_NAME = "pipeline_name"
SOURCE_PREFIX_VALUE = "data-egress-testing/2021-01-10/"
ROOT_PREFIX = "data-egress-testing/"
SOURCE_PREFIX_TODAYS_DATE_VALUE = (
    f'{ROOT_PREFIX}{datetime.datetime.today().strftime("%Y-%m-%d")}/'
)
SOURCE_PREFIX_GENERIC_VALUE = "data-egress-testing/dir1/dir2/"
RECIPIENT_NAME_VALUE = "OpsMI"
S3_TRANSFER_TYPE_VALUE = "S3"
DESTINATION_BUCKET_VALUE = "4321"
SOURCE_BUCKET_VALUE = "1234"
AWS_REGION = "us-east-1"
DYNAMODB_TABLENAME = "data-egress"


def test_get_to_be_processed_s3_prefixes():
    json_file = open("tests/sqs_message.json")
    message_body = json.load(json_file)
    message = {BODY: json.dumps(message_body)}
    s3_prefixes = sqs_listener.get_to_be_processed_s3_prefixes(message)
    assert SOURCE_PREFIX_VALUE == s3_prefixes[0]


def test_get_to_be_processed_s3_prefixes_with_invalid_msg():
    json_file = open("tests/sqs_message_no_records.json")
    message_body = json.load(json_file)
    message = {BODY: json.dumps(message_body)}
    with pytest.raises(KeyError) as ex:
        sqs_listener.get_to_be_processed_s3_prefixes(message)
    assert (
        str(ex.value)
        == "\"Key: 's3' not found when retrieving the prefix from sqs message\""
    )


def test_get_to_be_processed_s3_prefixes_wrong_formatted_prefix_1():
    json_file = open("tests/sqs_message_wrong_formatted_prefix_1.json")
    message_body = json.load(json_file)
    message = {BODY: json.dumps(message_body)}
    s3_prefixes = sqs_listener.get_to_be_processed_s3_prefixes(message)
    assert len(s3_prefixes) == 0


def test_get_to_be_processed_s3_prefixes_wrong_formatted_prefix_2():
    json_file = open("tests/sqs_message_wrong_formatted_prefix_2.json")
    message_body = json.load(json_file)
    message = {BODY: json.dumps(message_body)}
    s3_prefixes = sqs_listener.get_to_be_processed_s3_prefixes(message)
    assert len(s3_prefixes) == 0


@mock_sqs
@mock_dynamodb2
@mock_s3
@mock_sts
def test_all(monkeypatch, aws_credentials):
    sqs_client = boto3.client(service_name=SERVICE_SQS, region_name=AWS_REGION)
    json_file = open("tests/sqs_message.json")
    response = json.load(json_file)
    msg_json_str = json.dumps(response)
    args = mock_args()
    args.sqs_url = mock_get_sqs_resource().url
    args.region_name = AWS_REGION
    sqs_client.send_message(QueueUrl=args.sqs_url, MessageBody=msg_json_str)
    monkeypatch.setattr(
        sqs_listener, "get_dynamodb_resource", mock_get_dynamodb_resource
    )
    monkeypatch.setattr(sqs_listener, "call_dks", mock_call_dks)
    s3_client = mock_get_s3_client(SOURCE_PREFIX_VALUE)
    sqs_listener.listen(args, s3_client)
    compressed_data = s3_client.get_object(
        Bucket=DESTINATION_BUCKET_VALUE, Key=f"{DESTINATION_PREFIX_VALUE}some_file"
    )[BODY].read()
    decompressed = decompress(compressed_data).decode()
    response = sqs_client.get_queue_attributes(
        QueueUrl=args.sqs_url, AttributeNames=[NUMBER_OF_MESSAGES]
    )
    available_msg_count = int(response[ATTRIBUTES][NUMBER_OF_MESSAGES])
    assert available_msg_count == 0
    assert decompressed == TEST_DATA


@mock_sqs
@mock_dynamodb2
@mock_s3
@mock_sts
def test_todays_date(monkeypatch, aws_credentials):
    sqs_client = boto3.client(service_name=SERVICE_SQS, region_name=AWS_REGION)
    json_file = open("tests/sqs_message.json")
    response = json.load(json_file)
    response["Records"][0]["s3"]["object"][
        "key"
    ] = f"{SOURCE_PREFIX_TODAYS_DATE_VALUE}pipeline_success.flag"
    msg_json_str = json.dumps(response)
    args = mock_args()
    args.sqs_url = mock_get_sqs_resource().url
    args.region_name = AWS_REGION
    sqs_client.send_message(QueueUrl=args.sqs_url, MessageBody=msg_json_str)
    monkeypatch.setattr(
        sqs_listener, "get_dynamodb_resource", mock_get_dynamodb_resource_date_prefix
    )
    monkeypatch.setattr(sqs_listener, "call_dks", mock_call_dks)
    s3_client = mock_get_s3_client(SOURCE_PREFIX_TODAYS_DATE_VALUE)
    sqs_listener.listen(args, s3_client)
    compressed_data = s3_client.get_object(
        Bucket=DESTINATION_BUCKET_VALUE, Key=f"{DESTINATION_PREFIX_VALUE}some_file"
    )[BODY].read()
    decompressed = decompress(compressed_data).decode()
    response = sqs_client.get_queue_attributes(
        QueueUrl=args.sqs_url, AttributeNames=[NUMBER_OF_MESSAGES]
    )
    available_msg_count = int(response[ATTRIBUTES][NUMBER_OF_MESSAGES])
    assert available_msg_count == 0
    assert decompressed == TEST_DATA


@mock_sqs
@mock_dynamodb2
@mock_s3
@mock_sts
def test_todays_generic(monkeypatch, aws_credentials):
    sqs_client = boto3.client(service_name=SERVICE_SQS, region_name=AWS_REGION)
    json_file = open("tests/sqs_message_generic.json")
    response = json.load(json_file)
    msg_json_str = json.dumps(response)
    args = mock_args()
    args.sqs_url = mock_get_sqs_resource().url
    args.region_name = AWS_REGION
    sqs_client.send_message(QueueUrl=args.sqs_url, MessageBody=msg_json_str)
    monkeypatch.setattr(
        sqs_listener, "get_dynamodb_resource", mock_get_dynamodb_resource_generic_prefix
    )
    monkeypatch.setattr(sqs_listener, "call_dks", mock_call_dks)
    s3_client = mock_get_s3_client(SOURCE_PREFIX_GENERIC_VALUE)
    sqs_listener.listen(args, s3_client)
    compressed_data = s3_client.get_object(
        Bucket=DESTINATION_BUCKET_VALUE,
        Key=f"{DESTINATION_PREFIX_VALUE}{SOURCE_PREFIX_GENERIC_VALUE.replace(ROOT_PREFIX, '')}some_file",
    )[BODY].read()
    decompressed = decompress(compressed_data).decode()
    response = sqs_client.get_queue_attributes(
        QueueUrl=args.sqs_url, AttributeNames=[NUMBER_OF_MESSAGES]
    )
    available_msg_count = int(response[ATTRIBUTES][NUMBER_OF_MESSAGES])
    assert available_msg_count == 0
    assert decompressed == TEST_DATA


@mock_sqs
@mock_dynamodb2
@mock_s3
@mock_sts
def test_todays_generic_for_encrypted_data(monkeypatch, aws_credentials):
    sqs_client = boto3.client(service_name=SERVICE_SQS, region_name=AWS_REGION)
    json_file = open("tests/sqs_message_generic.json")
    response = json.load(json_file)
    msg_json_str = json.dumps(response)
    args = mock_args()
    args.sqs_url = mock_get_sqs_resource().url
    args.region_name = AWS_REGION
    sqs_client.send_message(QueueUrl=args.sqs_url, MessageBody=msg_json_str)
    monkeypatch.setattr(
        sqs_listener,
        "get_dynamodb_resource",
        mock_get_dynamodb_resource_generic_prefix_decrypt_false,
    )
    monkeypatch.setattr(sqs_listener, "call_dks", mock_call_dks)
    s3_client = mock_get_s3_client_prefix_decrypt_false(SOURCE_PREFIX_GENERIC_VALUE)
    sqs_listener.listen(args, s3_client)
    encrypted_data = s3_client.get_object(
        Bucket=DESTINATION_BUCKET_VALUE,
        Key=f"{DESTINATION_PREFIX_VALUE}{SOURCE_PREFIX_GENERIC_VALUE.replace(ROOT_PREFIX, '')}some_encrypted_file.enc",
    )[BODY].read()

    response = sqs_client.get_queue_attributes(
        QueueUrl=args.sqs_url, AttributeNames=[NUMBER_OF_MESSAGES]
    )
    available_msg_count = int(response[ATTRIBUTES][NUMBER_OF_MESSAGES])
    assert available_msg_count == 0
    assert encrypted_data == encrypt_data(TEST_DATA)


@mock_sqs
def mock_get_sqs_resource():
    sqs = boto3.resource(service_name=SERVICE_SQS, region_name=AWS_REGION)
    test_sqs_queue = sqs.create_queue(QueueName=TEST_SQS_QUEUE)
    return test_sqs_queue


@mock_dynamodb2
def mock_get_dynamodb_resource(region_name):
    dynamodb = boto3.resource(service_name=SERVICE_DYNAMODB, region_name=AWS_REGION)
    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLENAME,
        KeySchema=[
            {"AttributeName": KEY_SOURCE_PREFIX, "KeyType": "HASH"},  # Partition key
            {"AttributeName": KEY_PIPELINE_NAME, "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": KEY_SOURCE_PREFIX, "AttributeType": "S"},
            {"AttributeName": KEY_PIPELINE_NAME, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    table.put_item(
        Item={
            KEY_SOURCE_PREFIX: SOURCE_PREFIX_VALUE,
            KEY_PIPELINE_NAME: RECIPIENT_NAME_VALUE,
            KEY_SOURCE_BUCKET: SOURCE_BUCKET_VALUE,
            KEY_DESTINATION_BUCKET: DESTINATION_BUCKET_VALUE,
            KEY_DESTINATION_PREFIX: DESTINATION_PREFIX_VALUE,
            KEY_TRANSFER_TYPE: S3_TRANSFER_TYPE_VALUE,
            KEY_RECIPIENT_NAME: RECIPIENT_NAME_VALUE,
            KEY_COMPRESS: True,
            KEY_DECRYPT: True,
            KEY_COMPRESSION_FMT: GZIP_VALUE,
            KEY_ROLE_ARN: ROLE_ARN_VALUE,
        }
    )
    return dynamodb


@mock_dynamodb2
def mock_get_dynamodb_resource_date_prefix(region_name):
    dynamodb = boto3.resource(service_name=SERVICE_DYNAMODB, region_name=AWS_REGION)
    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLENAME,
        KeySchema=[
            {"AttributeName": KEY_SOURCE_PREFIX, "KeyType": "HASH"},  # Partition key
            {"AttributeName": KEY_PIPELINE_NAME, "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": KEY_SOURCE_PREFIX, "AttributeType": "S"},
            {"AttributeName": KEY_PIPELINE_NAME, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    table.put_item(
        Item={
            KEY_SOURCE_PREFIX: f"{ROOT_PREFIX}$TODAYS_DATE/",
            KEY_PIPELINE_NAME: RECIPIENT_NAME_VALUE,
            KEY_SOURCE_BUCKET: SOURCE_BUCKET_VALUE,
            KEY_DESTINATION_BUCKET: DESTINATION_BUCKET_VALUE,
            KEY_DESTINATION_PREFIX: DESTINATION_PREFIX_VALUE,
            KEY_TRANSFER_TYPE: S3_TRANSFER_TYPE_VALUE,
            KEY_RECIPIENT_NAME: RECIPIENT_NAME_VALUE,
            KEY_COMPRESS: True,
            KEY_DECRYPT: True,
            KEY_COMPRESSION_FMT: GZIP_VALUE,
            KEY_ROLE_ARN: ROLE_ARN_VALUE,
        }
    )
    return dynamodb


@mock_dynamodb2
def mock_get_dynamodb_resource_generic_prefix(region_name):
    dynamodb = boto3.resource(service_name=SERVICE_DYNAMODB, region_name=AWS_REGION)
    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLENAME,
        KeySchema=[
            {"AttributeName": KEY_SOURCE_PREFIX, "KeyType": "HASH"},  # Partition key
            {"AttributeName": KEY_PIPELINE_NAME, "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": KEY_SOURCE_PREFIX, "AttributeType": "S"},
            {"AttributeName": KEY_PIPELINE_NAME, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    table.put_item(
        Item={
            KEY_SOURCE_PREFIX: f"{ROOT_PREFIX}*",
            KEY_PIPELINE_NAME: RECIPIENT_NAME_VALUE,
            KEY_SOURCE_BUCKET: SOURCE_BUCKET_VALUE,
            KEY_DESTINATION_BUCKET: DESTINATION_BUCKET_VALUE,
            KEY_DESTINATION_PREFIX: DESTINATION_PREFIX_VALUE,
            KEY_TRANSFER_TYPE: S3_TRANSFER_TYPE_VALUE,
            KEY_RECIPIENT_NAME: RECIPIENT_NAME_VALUE,
            KEY_COMPRESS: True,
            KEY_DECRYPT: True,
            KEY_COMPRESSION_FMT: GZIP_VALUE,
            KEY_ROLE_ARN: ROLE_ARN_VALUE,
        }
    )
    return dynamodb


@mock_dynamodb2
def mock_get_dynamodb_resource_generic_prefix_decrypt_false(region_name):
    dynamodb = boto3.resource(service_name=SERVICE_DYNAMODB, region_name=AWS_REGION)
    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLENAME,
        KeySchema=[
            {"AttributeName": KEY_SOURCE_PREFIX, "KeyType": "HASH"},  # Partition key
            {"AttributeName": KEY_PIPELINE_NAME, "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": KEY_SOURCE_PREFIX, "AttributeType": "S"},
            {"AttributeName": KEY_PIPELINE_NAME, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )
    table.put_item(
        Item={
            KEY_SOURCE_PREFIX: f"{ROOT_PREFIX}*",
            KEY_PIPELINE_NAME: RECIPIENT_NAME_VALUE,
            KEY_SOURCE_BUCKET: SOURCE_BUCKET_VALUE,
            KEY_DESTINATION_BUCKET: DESTINATION_BUCKET_VALUE,
            KEY_DESTINATION_PREFIX: DESTINATION_PREFIX_VALUE,
            KEY_TRANSFER_TYPE: S3_TRANSFER_TYPE_VALUE,
            KEY_RECIPIENT_NAME: RECIPIENT_NAME_VALUE,
            KEY_COMPRESS: True,
            KEY_DECRYPT: False,
            KEY_COMPRESSION_FMT: GZIP_VALUE,
            KEY_ROLE_ARN: ROLE_ARN_VALUE,
        }
    )
    return dynamodb


@mock_s3
def mock_get_s3_client_prefix_decrypt_false(source_prefix):
    s3_client = boto3.client(service_name=SERVICE_S3, region_name=AWS_REGION)
    s3_client.create_bucket(Bucket=SOURCE_BUCKET_VALUE)
    s3_client.create_bucket(Bucket=DESTINATION_BUCKET_VALUE)
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ROLE_ARN_VALUE},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::4321",
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": ROLE_ARN_VALUE},
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": "arn:aws:s3:::4321/*",
            },
        ],
    }
    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)
    s3_client.put_bucket_policy(Bucket=DESTINATION_BUCKET_VALUE, Policy=bucket_policy)

    encrypted = encrypt_data(TEST_DATA)
    s3_client.put_object(
        Body=encrypted,
        Bucket=SOURCE_BUCKET_VALUE,
        Key=f"{source_prefix}some_encrypted_file.enc",
        Metadata={
            "iv": IV_BASE64,
            "ciphertext": "test_ciphertext",
            "datakeyencryptionkeyid": "123",
        },
    )
    s3_client.put_object(
        Bucket=SOURCE_BUCKET_VALUE, Key=f"{source_prefix}pipeline_success.flag"
    )
    return s3_client


@mock_s3
def mock_get_s3_client(source_prefix):
    s3_client = boto3.client(service_name=SERVICE_S3, region_name=AWS_REGION)
    s3_client.create_bucket(Bucket=SOURCE_BUCKET_VALUE)
    s3_client.create_bucket(Bucket=DESTINATION_BUCKET_VALUE)
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ROLE_ARN_VALUE},
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::4321",
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": ROLE_ARN_VALUE},
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": "arn:aws:s3:::4321/*",
            },
        ],
    }
    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)
    s3_client.put_bucket_policy(Bucket=DESTINATION_BUCKET_VALUE, Policy=bucket_policy)

    encrypted = encrypt_data(TEST_DATA)
    s3_client.put_object(
        Body=encrypted,
        Bucket=SOURCE_BUCKET_VALUE,
        Key=f"{source_prefix}some_file.enc",
        Metadata={
            "iv": IV_BASE64,
            "ciphertext": "test_ciphertext",
            "datakeyencryptionkeyid": "123",
        },
    )
    s3_client.put_object(
        Bucket=SOURCE_BUCKET_VALUE, Key=f"{source_prefix}pipeline_success.flag"
    )
    return s3_client


def decompress(data):
    return zlib.decompress(data, 16 + zlib.MAX_WBITS)


def encrypt_data(data):
    return encrypt(IV_INT, PLAIN_TEXT_KEY, data.encode())


def encrypt(initialisation_vector, datakey, unencrypted_bytes):
    counter = Counter.new(AES.block_size * 8, initial_value=initialisation_vector)
    aes = AES.new(base64.b64decode(datakey), AES.MODE_CTR, counter=counter)
    return aes.encrypt(unencrypted_bytes)


def mock_args():
    args = argparse.Namespace()
    args.log_level = "INFO"
    args.is_test = True
    args.environment = "Test"
    args.application = "data-egress"
    args.max_retries = 1
    return args


def create_iam_role():
    iam_client = boto3.client(SERVICE_IAM)
    trust_relationship_policy_another_iam_user = trust_relationship()
    role = create_role(iam_client, trust_relationship_policy_another_iam_user)
    policy_arn = create_policy(iam_client)
    attach_policy(iam_client, policy_arn)


# TODO https://aws.amazon.com/blogs/security/easily-control-naming-individual-iam-role-sessions/ - anaonymous is bad
def trust_relationship():
    trust_relationship_policy_another_iam_user = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}
        ],
    }
    return trust_relationship_policy_another_iam_user


def create_role(iam_client, trust_relationship_policy_another_iam_user):
    try:
        return iam_client.create_role(
            RoleName=DESTINATION_BUCKET_ROLE,
            AssumeRolePolicyDocument=json.dumps(
                trust_relationship_policy_another_iam_user
            ),
            Description="This is a test role for destination bucket",
        )
    except Exception as ex:
        print(f"Error while creating role {str(ex)}")


def create_policy(iam_client):
    policy_json = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::4321",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": "arn:aws:s3:::4321/*",
            },
        ],
    }

    policy_name = DESTINATION_BUCKET_ROLE + "_policy"
    try:
        policy_res = iam_client.create_policy(
            PolicyName=policy_name, PolicyDocument=json.dumps(policy_json)
        )
        policy_arn = policy_res["Policy"]["Arn"]
        return policy_arn
    except Exception as ex:
        print(f"Error while creating policy {str(ex)}")


def attach_policy(iam_client, policy_arn):
    try:
        iam_client.attach_role_policy(
            RoleName=DESTINATION_BUCKET_ROLE, PolicyArn=policy_arn
        )
    except Exception as ex:
        print(f"Error while attaching policy {str(ex)}")


def mock_call_dks(cek, kek, args):
    return PLAIN_TEXT_KEY
