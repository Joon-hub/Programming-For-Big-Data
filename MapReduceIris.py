#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sepW_virginica,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_sepW_virginica(self, _, line):
        # yield each sepal width
        data = DATA_RE.findall(line)
        if "Iris-virginica" in data:
            sep_W = float(data[1])
            yield ("sepal width", sep_W)

    def reducer_get_avg(self, key, values):
        # get max of the petal widths
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("virginica sepal width avg", round(total,1) / size)
if __name__ == '__main__':
    MRProb2_3.run()
