# -*- coding: utf-8 -*-


from mrjob.job import MRJob
from mrjob.step import MRStep

class UserMovieTagCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_user_movie,
                   reducer=self.reducer_count_tags_per_user_movie)
        ]

    def mapper_extract_user_movie(self, _, line):
        if line.startswith("userId"):
            return
        try:
            userId, movieId, tag, timestamp = line.strip().split(',', 3)
            key = (userId, movieId)
            yield key, 1
        except ValueError:
            pass  # ignore les lignes malformées

    def reducer_count_tags_per_user_movie(self, user_movie, counts):
        yield user_movie, sum(counts)

if __name__ == '__main__':
    UserMovieTagCount.run()
