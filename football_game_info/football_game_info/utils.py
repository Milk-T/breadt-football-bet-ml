


def is_blank(word):
    if word and word.strip():
        return False
    return True

def take_result(gs, gd):
    if gs > gd:
        return 2
    elif gs == gd:
        return 1
    else:
        return 0