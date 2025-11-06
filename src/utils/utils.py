from difflib import SequenceMatcher


def merge_with_overlap(chunks, overlap=0):
    """Smartly merge overlapping text chunks."""
    if not chunks:
        return ""

    merged = chunks[0]
    for next_chunk in chunks[1:]:
        # Try to find best overlap region (up to 'overlap' chars)
        # matcher = SequenceMatcher(None, merged[-overlap:], next_chunk[:overlap])
        # match = matcher.find_longest_match(0, overlap, 0, overlap)
        # if match.size > 0:
        #     merged += next_chunk[match.b:]
        # else:
        merged += next_chunk
    return merged