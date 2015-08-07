from mrjob.job import MRJob

class MREntropyPerURL(MRJob):
    # 1st MR: urlpath -> entropy([ips])
    def input_mapper(self, _, line):
        ip, path = line.split()
        yield path, ip

    def urlpath_to_entropy(self, key, values):
        yield key, entropy_bits(values)

    # 2nd MR: aggregate all urlpaths by same entropy_val
    def swap_values(self, key, value):
        yield value, key

    def values_per_key(self, key, values):
        yield key, " ".join(list(values))

    def steps(self):
        return [self.mr(mapper=self.input_mapper,
                        reducer=self.urlpath_to_entropy),
                self.mr(mapper=self.swap_values,
                        reducer=self.values_per_key)]

if __name__ == '__main__':
    MREntropyPerURL.run()
