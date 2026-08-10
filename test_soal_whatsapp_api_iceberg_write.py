import json
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import soal_whatsapp_api_iceberg_write as soal

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)


META_TEXT_PAYLOAD = load_fixture("meta_inbound_text.json")
META_IMAGE_PAYLOAD = load_fixture("meta_inbound_image.json")
META_IMAGE_HANDLER_RESPONSE = load_fixture("meta_image_handler_response.json")
META_OUTBOUND_STATUS_PAYLOAD = load_fixture("meta_outbound_status.json")
META_UNHANDLED_CHANGE_FIELD_PAYLOAD = load_fixture("meta_unhandled_change_field.json")

SPOKI_TEXT_PAYLOAD = load_fixture("spoki_inbound_text.json")
SPOKI_OUTBOUND_TEXT_PAYLOAD = load_fixture("spoki_outbound_text.json")
SPOKI_TEXT_MISSING_TIMESTAMP_MS_PAYLOAD = load_fixture("spoki_inbound_text_missing_timestamp_ms.json")
SPOKI_UNHANDLED_CONTENT_TYPE_PAYLOAD = load_fixture("spoki_inbound_unhandled_content_type.json")
SPOKI_UNHANDLED_EVENT_TYPE_PAYLOAD = load_fixture("spoki_unhandled_event_type.json")
SPOKI_UNRECOGNIZED_DIRECTION_PAYLOAD = load_fixture("spoki_outbound_unrecognized_direction.json")
SPOKI_IMAGE_PAYLOAD = load_fixture("spoki_inbound_image.json")
SPOKI_IMAGE_WITH_CAPTION_PAYLOAD = load_fixture("spoki_inbound_image_with_caption.json")
SPOKI_IMAGE_NO_MEDIA_PAYLOAD = load_fixture("spoki_inbound_image_no_media.json")

SCHEMA_COLUMNS = set(field.name for field in soal.ICEBERG_TABLE_SCHEMA.fields)


class DetectVendorTests(unittest.TestCase):
    def test_meta_payload(self):
        self.assertEqual(soal.detect_vendor(META_TEXT_PAYLOAD), "meta")

    def test_spoki_payload(self):
        self.assertEqual(soal.detect_vendor(SPOKI_TEXT_PAYLOAD), "spoki")

    def test_unrecognized_dict(self):
        self.assertEqual(soal.detect_vendor({"foo": "bar"}), "unknown")

    def test_non_dict_input(self):
        self.assertEqual(soal.detect_vendor(["not", "a", "dict"]), "unknown")
        self.assertEqual(soal.detect_vendor(None), "unknown")


class BuildMetaRecordsTests(unittest.TestCase):
    def test_text_message(self):
        records = soal.build_meta_records(META_TEXT_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["phonenumber"], "256774768254")
        self.assertEqual(record["recipientphonenumber"], "256200970028")
        self.assertEqual(
            record["message_id"],
            "wamid.HBgMMjU2Nzc0NzY4MjU0FQIAEhggQTUxMkM0QUVBRTc4QTcxM0Y4RjNCQkUyNUU0NzkwQUMA",
        )
        self.assertEqual(record["text_content"], "Hello")
        self.assertEqual(record["direction"], "inbound")
        self.assertEqual(record["type"], "text")
        self.assertEqual(record["timestamp"], 1783665947)
        self.assertIsInstance(record["timestamp"], int)

    @patch("soal_whatsapp_api_iceberg_write.handle_image_message")
    def test_image_message_fills_missing_fields(self, mock_handle_image):
        mock_handle_image.return_value = META_IMAGE_HANDLER_RESPONSE

        records = soal.build_meta_records(META_IMAGE_PAYLOAD)

        mock_handle_image.assert_called_once()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["image_url"], "https://example.com/image.jpg")
        self.assertIsNone(record["text_content"])
        self.assertIsNone(record["status"])

    def test_outbound_status_with_conversation_and_pricing(self):
        records = soal.build_meta_records(META_OUTBOUND_STATUS_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["direction"], "outbound")
        self.assertEqual(record["status"], "delivered")
        self.assertEqual(record["conversation_id"], "conv1")
        self.assertEqual(record["conversation_type"], "service")
        self.assertEqual(record["pricing_model"], "CBP")
        self.assertEqual(record["pricing_category"], "service")

    def test_ignores_non_messages_field(self):
        self.assertEqual(soal.build_meta_records(META_UNHANDLED_CHANGE_FIELD_PAYLOAD), [])

    def test_non_meta_payload_returns_empty(self):
        self.assertEqual(soal.build_meta_records({"foo": "bar"}), [])


class BuildSpokiRecordsTests(unittest.TestCase):
    def test_text_message(self):
        records = soal.build_spoki_records(SPOKI_TEXT_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["phonenumber"], "+256703030436")
        self.assertEqual(record["recipientphonenumber"], "256759712768")
        self.assertEqual(record["message_id"], "6b9a2ed0e79e5f32386d6af83e3d2e6b")
        self.assertEqual(record["text_content"], "Ok")
        self.assertEqual(record["direction"], "inbound")
        self.assertEqual(record["type"], "text")
        self.assertEqual(record["timestamp"], 1785783756)
        self.assertIsInstance(record["timestamp"], int)

    def test_falls_back_to_top_level_timestamp_when_timestamp_ms_missing(self):
        records = soal.build_spoki_records(SPOKI_TEXT_MISSING_TIMESTAMP_MS_PAYLOAD)

        self.assertEqual(records[0]["timestamp"], 1785783757)

    def test_unhandled_content_type_is_metadata_only(self):
        records = soal.build_spoki_records(SPOKI_UNHANDLED_CONTENT_TYPE_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["type"], "video")
        self.assertIsNone(record["text_content"])

    @patch("soal_whatsapp_api_iceberg_write.handle_spoki_image_message")
    def test_image_message_downloads_and_uploads_to_s3(self, mock_handle_image):
        mock_handle_image.return_value = {
            "text_content": None,
            "image_url": "https://example.com/spoki-image.jpg",
            "image_mime_type": "image/jpeg",
            "image_sha256": None,
            "s3_image_path": "images/+256709079019/123_3b7514ce25fac88166d911a0f9938fd1.jpg",
        }

        records = soal.build_spoki_records(SPOKI_IMAGE_PAYLOAD)

        mock_handle_image.assert_called_once_with(
            SPOKI_IMAGE_PAYLOAD["data"], "3b7514ce25fac88166d911a0f9938fd1", "+256709079019"
        )
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["type"], "image")
        self.assertEqual(record["image_url"], "https://example.com/spoki-image.jpg")
        self.assertEqual(record["image_mime_type"], "image/jpeg")
        self.assertEqual(record["s3_image_path"], "images/+256709079019/123_3b7514ce25fac88166d911a0f9938fd1.jpg")
        self.assertIsNone(record["image_sha256"])

    def test_image_with_no_media_is_metadata_only(self):
        records = soal.build_spoki_records(SPOKI_IMAGE_NO_MEDIA_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["type"], "image")
        self.assertIsNone(record["image_url"])
        self.assertIsNone(record["s3_image_path"])
        self.assertIsNone(record["text_content"])

    def test_unhandled_event_type_returns_no_records(self):
        self.assertEqual(soal.build_spoki_records(SPOKI_UNHANDLED_EVENT_TYPE_PAYLOAD), [])

    def test_outbound_text_message(self):
        records = soal.build_spoki_records(SPOKI_OUTBOUND_TEXT_PAYLOAD)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record.keys()), SCHEMA_COLUMNS)
        self.assertEqual(record["phonenumber"], "256759712768")
        self.assertEqual(record["recipientphonenumber"], "+256785258455")
        self.assertEqual(record["message_id"], "b505e5cf52264b5bb660f768bf389db1")
        self.assertEqual(record["text_content"], "Hi Jacob, How may I help you ?")
        self.assertEqual(record["direction"], "outbound")
        self.assertEqual(record["type"], "text")
        self.assertEqual(record["status"], "sent")
        self.assertEqual(record["timestamp"], 1785749305)
        self.assertIsInstance(record["timestamp"], int)

    def test_inbound_still_has_no_status(self):
        records = soal.build_spoki_records(SPOKI_TEXT_PAYLOAD)

        self.assertIsNone(records[0]["status"])

    def test_unrecognized_direction_logs_warning_but_still_records(self):
        records = soal.build_spoki_records(SPOKI_UNRECOGNIZED_DIRECTION_PAYLOAD)

        self.assertEqual(len(records), 1)
        self.assertIsNone(records[0]["status"])
        self.assertEqual(records[0]["direction"], "sideways")


class HandleSpokiImageMessageTests(unittest.TestCase):
    @patch("soal_whatsapp_api_iceberg_write.upload_image_to_s3")
    @patch("soal_whatsapp_api_iceberg_write.download_media_from_url")
    def test_success_populates_image_fields(self, mock_download, mock_upload):
        mock_download.return_value = b"fake-image-bytes"
        mock_upload.return_value = ("https://example.com/s3-image.jpg", "images/+256709079019/1_x.jpg")

        data = SPOKI_IMAGE_PAYLOAD["data"]
        updates = soal.handle_spoki_image_message(data, "msg1", "+256709079019")

        mock_download.assert_called_once_with(data["mediamessage_set"][0]["media"])
        mock_upload.assert_called_once_with(b"fake-image-bytes", "msg1", "+256709079019", "image/jpeg")
        self.assertEqual(updates["image_url"], "https://example.com/s3-image.jpg")
        self.assertEqual(updates["s3_image_path"], "images/+256709079019/1_x.jpg")
        self.assertEqual(updates["image_mime_type"], "image/jpeg")
        self.assertIsNone(updates["image_sha256"])
        self.assertIsNone(updates["text_content"])

    @patch("soal_whatsapp_api_iceberg_write.upload_image_to_s3")
    @patch("soal_whatsapp_api_iceberg_write.download_media_from_url")
    def test_download_failure_still_sets_mime_type(self, mock_download, mock_upload):
        mock_download.return_value = None

        data = SPOKI_IMAGE_PAYLOAD["data"]
        updates = soal.handle_spoki_image_message(data, "msg1", "+256709079019")

        mock_upload.assert_not_called()
        self.assertIsNone(updates["image_url"])
        self.assertIsNone(updates["s3_image_path"])
        self.assertEqual(updates["image_mime_type"], "image/jpeg")

    @patch("soal_whatsapp_api_iceberg_write.upload_image_to_s3")
    @patch("soal_whatsapp_api_iceberg_write.download_media_from_url")
    def test_empty_mediamessage_set_skips_download(self, mock_download, mock_upload):
        data = SPOKI_IMAGE_NO_MEDIA_PAYLOAD["data"]
        updates = soal.handle_spoki_image_message(data, "msg1", "+256703030436")

        mock_download.assert_not_called()
        mock_upload.assert_not_called()
        self.assertIsNone(updates["image_url"])
        self.assertIsNone(updates["image_mime_type"])
        self.assertIsNone(updates["s3_image_path"])

    @patch("soal_whatsapp_api_iceberg_write.upload_image_to_s3")
    @patch("soal_whatsapp_api_iceberg_write.download_media_from_url")
    def test_caption_is_captured(self, mock_download, mock_upload):
        mock_download.return_value = b"fake-image-bytes"
        mock_upload.return_value = ("https://example.com/s3-image.jpg", "images/+256709079019/1_x.jpg")

        data = SPOKI_IMAGE_WITH_CAPTION_PAYLOAD["data"]
        updates = soal.handle_spoki_image_message(data, "msg2", "+256709079019")

        self.assertEqual(updates["text_content"], "Here is my payment proof")


class HandleMessageEventDispatchTests(unittest.TestCase):
    @patch("soal_whatsapp_api_iceberg_write.write_to_iceberg")
    def test_routes_meta_payload(self, mock_write):
        result = soal.handle_message_event(META_TEXT_PAYLOAD, spark=None)

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"])["records_processed"], 1)
        mock_write.assert_called_once()
        written_records, written_spark = mock_write.call_args[0]
        self.assertEqual(len(written_records), 1)
        self.assertIsNone(written_spark)

    @patch("soal_whatsapp_api_iceberg_write.write_to_iceberg")
    def test_routes_spoki_payload(self, mock_write):
        result = soal.handle_message_event(SPOKI_TEXT_PAYLOAD, spark=None)

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"])["records_processed"], 1)
        mock_write.assert_called_once()

    @patch("soal_whatsapp_api_iceberg_write.write_to_iceberg")
    def test_unknown_payload_writes_nothing(self, mock_write):
        result = soal.handle_message_event({"foo": "bar"}, spark=None)

        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"])["records_processed"], 0)
        mock_write.assert_not_called()

    @patch("soal_whatsapp_api_iceberg_write.build_meta_records", side_effect=RuntimeError("boom"))
    def test_exception_returns_500(self, _mock_build):
        result = soal.handle_message_event(META_TEXT_PAYLOAD, spark=None)

        self.assertEqual(result["statusCode"], 500)


if __name__ == "__main__":
    unittest.main()
