# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep

class TagFrequencyCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_extract_tags(self, _, line):
        # Ignore l'en-tête
        if line.startswith("userId"):
            return
        try:
            userId, movieId, tag, timestamp = line.strip().split(',', 3)
            yield tag.lower().strip(), 1  # tag en minuscules et nettoyé
        except ValueError:
            pass  # Ignore les lignes mal formées

    def reducer_count_tags(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagFrequencyCounter.run()
