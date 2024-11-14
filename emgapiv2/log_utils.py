from django.conf import settings


def mask_sensitive_data(text: str) -> str:
    for pattern in settings.EMG_CONFIG.log_masking.patterns:
        text = pattern.match.sub(pattern.replacement, text)

    return text
