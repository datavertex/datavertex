def text_to_float(text):
    t = text
    dot_pos = t.rfind('.')
    comma_pos = t.rfind(',')
    if comma_pos > dot_pos:
        t = t.replace(".", "")
        t = t.replace(",", ".")
    else:
        t = t.replace(",", "")

    return(float(t))