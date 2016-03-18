pearsonMatrix/ contains 30 lines of sample Pearson correlation matrix
recommendationResults/ contains sample recommendation results for a user

user109 is a sample user rating vector
user109_nor is a sample normalized user rating vector

make filter:
make
use filter:
./filter inputUserRatingsData outputFile threshold

run the complete flow locally:
./runlocal inputUserRatingsData targetUser_normalized
run only the recommendation for a specific user locally (must have artist-user matrix and artist average rating vector localy):
./step6 targetUser

run the compelte flow on Comet:
./run inputUserRatingsData targetUser_normalized

normalizeRatingVector.py & normalizeRatingVector_AnotherFormat.py are used to calculate normalized user rating vector
usage:
python normalizeRatingVector_AnotherFormat.py user109 user109_nor

