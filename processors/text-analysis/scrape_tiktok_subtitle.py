import time
import requests
import json
import re
from backend.lib.processor import BasicProcessor

class TikTokSubtitleProcessor(BasicProcessor):
    type = "subtitle-processor-tiktok"
    category = "Textual"
    title = "Download and Process TikTok Subtitles"
    description = "Downloads WebVTT subtitle files for TikTok videos and integrates processed content into the dataset."

    options = {
        "amount": {
            "type": "number",
            "default": 100,
            "min": 0,
            "max": 1000,
            "help": "Max number of subtitles to process.",
        },
        "language": {
            "type": "text",
            "default": "en",
            "help": "Language code (e.g., 'en' for English).",
        },
        "strip_timestamps": {
            "type": "boolean",
            "default": False,
            "help": "Remove timestamps and formatting from subtitle content.",
        },
    }

    def process(self):
        if self.source_dataset.num_rows == 0:
            self.dataset.update_status("No data to process.", is_final=True)
            self.dataset.finish(0)
            return

        # Retrieve parameters
        amount = self.parameters.get("amount", self.source_dataset.num_rows)
        max_amount = min(amount, self.source_dataset.num_rows)
        preferred_language = self.parameters.get("language", "en").strip()
        strip_timestamps = self.parameters.get("strip_timestamps", False)
        current_time = int(time.time())

        # Prepare to process items
        updated_items = []
        processed_count = 0

        self.dataset.update_status("Processing TikTok subtitles")

        for item in self.source_dataset.iterate_items(self):
            if processed_count >= max_amount:
                break

            video_id = item.get("id", "unknown")
            subtitles = item.get("video", {}).get("subtitleInfos", [])
            subtitle_content = None

            # Filter subtitles by language and format
            matching_subtitles = [
                sub for sub in subtitles
                if sub.get("LanguageCodeName", "").startswith(preferred_language) and sub.get("Format") == "webvtt"
            ]

            if matching_subtitles:
                url_info = matching_subtitles[0]
                url = url_info.get("Url")
                url_expire = url_info.get("UrlExpire", 0)

                if current_time > url_expire:
                    self.logger.info(f"URL expired for video {video_id}. Skipping download.")
                    subtitle_content = "Expired URL"
                elif url:
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        subtitle_content = self.parse_webvtt(response.text, strip_timestamps)
                    except requests.RequestException as e:
                        self.logger.warning(f"Failed to download subtitles for video {video_id}: {e}")

            updated_item = item.copy()
            updated_item["subtitles_content"] = subtitle_content or None
            updated_items.append(updated_item)
            processed_count += 1

            # Update progress periodically
            if processed_count % 10 == 0:
                self.dataset.update_progress(processed_count / max_amount)

        # Save updated items to the dataset
        self.dataset.update_items(updated_items)
        self.dataset.update_status("Processing complete.", is_final=True)
        self.dataset.finish(len(updated_items))

    def parse_webvtt(self, content, strip_timestamps):
        """
        Parses WebVTT content based on the strip_timestamps option.
        :param content: Raw WebVTT file content as a string
        :param strip_timestamps: Boolean indicating whether to remove timestamps
        :return: Parsed subtitle content as a string
        """
        if not strip_timestamps:
            return content

        parsed_content = []
        for line in content.splitlines():
            # Skip timestamp lines (basic pattern match for "HH:MM:SS.mmm --> HH:MM:SS.mmm")
            if re.match(r"^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}$", line):
                continue
            # Skip WebVTT headers
            if line.startswith("WEBVTT") or line.strip() == "":
                continue
            parsed_content.append(line)

        return "\n".join(parsed_content)
