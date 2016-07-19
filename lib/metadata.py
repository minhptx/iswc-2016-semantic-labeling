__author__ = 'alse'


class MetaData:
    def __init__(self, meta_text):
        data = meta_text.split(",")
        self.label = data[0].replace("_", "")
        self.length = int(data[1])
        self.size = int(data[2])

    def get_label(self):
        return self.label

    def get_length(self):
        return self.length

    def get_size(self):
        return self.size
