"""
Utils package for the airplanes project.
"""

def sanitize_topic(topic):
    """
    Sanitize MQTT topic by replacing special characters.

    The plus sign `+`, the hash sign `#`, and spaces are replaced by `-`.
    Umlauts are replaced (e.g., ä with ae).

    Args:
        topic (str): The topic to sanitize

    Returns:
        str: The sanitized topic
    """
    replacements = {
        '+': '-',
        '#': '-',
        ' ': '-',
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue',
        'ß': 'ss'
    }

    for char, replacement in replacements.items():
        topic = topic.replace(char, replacement)

    return topic
