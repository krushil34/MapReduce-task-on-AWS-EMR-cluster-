# This script determines Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRevenuePickupLocation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7] # Extract pickup location ID
            revenue = float(fields[16]) # Extract trip revenue
            yield pickup_location, revenue

    def reducer(self, pickup_location, revenues):
        # Sum up all revenues for each pickup location
        yield None, (sum(revenues), pickup_location)

    def final_reducer(self, _, max_revenues):
        # Find the pickup location with the highest revenue
        max_revenue, pickup_location = max(max_revenues)
        yield pickup_location, max_revenue


if __name__ == '__main__':
    MostRevenuePickupLocation.run()