# base class for writing map reduce jobs
from mrjob.job import MRJob

# base class for writing map reduce steps
from mrjob.step import MRStep

# regular expression for parsing data
import re

DATA_RE = re.compile(r"[\w.-]+")

#The class MRProb2_3 inherits from MRJob, making it a MapReduce job.
class MRAverageHoursWorked(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_hours,
                   reducer=self.reducer_compute_average)
        ]
    
    def mapper_extract_hours(self, _, line):
        """Extract income class and hours worked per week"""
        data = line.strip().split(',') # Split by comma and space
        if len(data) < 15:
            return
        income_class = data[14] # 14th column is income class
        hours_per_week = int(data[12]) # 12th column is hours per week
        yield (income_class, hours_per_week)

    def reducer_compute_average(self, income_class, hours_per_week):
        """Compute average hours worked per week for each income class"""
        total_hours = 0
        count = 0
        for hour in hours_per_week:
            total_hours += hour
            count += 1
        yield (income_class, total_hours / count)

if __name__ == "__main__":
    MRAverageHoursWorked.run()
