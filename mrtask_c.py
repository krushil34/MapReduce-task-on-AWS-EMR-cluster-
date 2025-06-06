# This script determines What are the different payment types used by customers and their count? The final results is in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypesCount(MRJob):

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            payment_type = fields[9] # Extract payment type
            yield payment_type, 1

    def combiner(self, payment_type, counts):
        # Local aggregation before sending data to the reducer
        yield payment_type, sum(counts)

    def reducer(self, payment_type, counts):
        # Sum counts for each payment type
        yield payment_type, sum(counts)

    def reducer_sort_results(self, payment_type, counts):
        # Prepare for sorting results
        yield None, (sum(counts), payment_type)

    def reducer_output_result(self, _, sorted_results):
        # Sort results in descending order
        for count, payment_type in sorted(sorted_results, reverse=True):
            yield payment_type, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.reducer_sort_results),
            MRStep(reducer=self.reducer_output_result)
        ]

if __name__ == '__main__':
    PaymentTypesCount.run()