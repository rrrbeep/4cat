"""
Generate interval-based Word2Vec models for sentences
"""
import zipfile
import shutil
import pickle
import json

from gensim.models import Word2Vec, Phrases
from pathlib import Path

from backend.lib.helpers import UserInput, convert_to_int
from backend.abstract.processor import BasicProcessor
from backend.lib.exceptions import ProcessorInterruptedException

__author__ = "Sal Hagen"
__credits__ = ["Sal Hagen", "Stijn Peeters", "Tom Willaert"]
__maintainer__ = "Sal Hagen"
__email__ = "4cat@oilab.eu"


class GenerateWord2Vec(BasicProcessor):
	"""
	Generate Word2Vec models
	"""
	type = "generate-word2vec"  # job type ID
	category = "Text analysis"  # category
	title = "Generate Word2Vec models"  # title displayed in UI
	description = "Generates Word2Vec word embedding models for the sentences, per chosen time interval. These can then be used to analyse semantic word associations within the corpus. Note that good models require large(r) datasets."  # description displayed in UI
	extension = "zip"  # extension of result file, used internally and in UI

	accepts = ["tokenise-posts"]

	input = "zip"
	output = "zip"

	references = [
		"[Mikolov, Tomas, Ilya Sutskever, Kai Chen, Greg Corrado, and Jeffrey Dean. 2013. “Distributed Representations of Words and Phrases and Their Compositionality.” Advances in Neural Information Processing Systems, 2013: 3111-3119.](https://papers.nips.cc/paper/5021-distributed-representations-of-words-and-phrases-and-their-compositionality.pdf)",
		"[Mikolov, Tomas, Kai Chen, Greg Corrado, and Jeffrey Dean. 2013. “Efficient Estimation of Word Representations in Vector Space.” ICLR Workshop Papers, 2013: 1-12.](https://arxiv.org/pdf/1301.3781.pdf)",
		"[word2vec - Google Code](https://code.google.com/archive/p/word2vec/)",
		"[word2vec - Gensim documentation](https://radimrehurek.com/gensim/models/word2vec.html)",
		"[A Beginner's Guide to Word Embedding with Gensim Word2Vec Model - Towards Data Science](https://towardsdatascience.com/a-beginners-guide-to-word-embedding-with-gensim-word2vec-model-5970fa56cc92)"
	]

	options = {
		"algorithm": {
			"type": UserInput.OPTION_CHOICE,
			"default": "skipgram",
			"options": {
				"cbow": "Continuous Bag of Words (CBOW)",
				"skipgram": "Skip-gram"
			},
			"help": "Training algorithm",
			"tooltip": "See processor references for a more detailed explanation."
		},
		"window": {
			"type": UserInput.OPTION_CHOICE,
			"default": 5,
			"options": {"3": 3, "4": 4, "5": 5, "6": 6, "7": 7},
			"help": "Window",
			"tooltip": "Maximum distance between the current and predicted word within a sentence"
		},
		"dimensionality": {
			"type": UserInput.OPTION_TEXT,
			"default": 100,
			"min": 50,
			"max": 1000,
			"help": "Dimensionality of the word vectors"
		},
		"negative": {
			"type": UserInput.OPTION_TOGGLE,
			"default": False,
			"help": "Use negative sampling"
		},
		"min_count": {
			"type": UserInput.OPTION_TEXT,
			"default": 1,
			"help": "Minimum word occurrence",
			"tooltip": "How often a word should occur in the corpus to be included"
		}
	}

	def process(self):
		"""
		This takes a 4CAT results file as input, and outputs a number of files containing
		tokenised posts, grouped per time unit as specified in the parameters.
		"""
		self.dataset.update_status("Processing sentences")

		use_skipgram = 1 if self.parameters.get("algorithm") == "skipgram" else 0
		window = min(10, max(1, convert_to_int(self.parameters.get("window"), self.options["window"]["default"])))
		use_negative = 5 if self.parameters.get("negative") else 0
		min_count = max(1, convert_to_int(self.parameters.get("min_count"), self.options["min_count"]["default"]))
		dimensionality = convert_to_int(self.parameters.get("dimensionality"), 100)

		# prepare staging area
		temp_path = self.dataset.get_temporary_path()
		temp_path.mkdir()

		# go through all archived token sets and vectorise them
		models = 0
		with zipfile.ZipFile(self.source_file, "r") as token_archive:
			token_sets = token_archive.namelist()

			# create one model file per token file
			for token_set in token_sets:
				if self.interrupted:
					raise ProcessorInterruptedException("Interrupted while processing token sets")

				# the model file's name will be based on the token set name,
				# i.e. 2020-08-01.json becomes 2020-08-01.model
				token_set_name = token_set.split("/")[-1]

				# temporarily extract file (we cannot use ZipFile.open() as it doesn't support binary modes)
				temp_file = temp_path.joinpath(token_set_name)
				token_archive.extract(token_set_name, temp_path)

				# use the "list of lists" as input for the word2vec model
				# by default the tokeniser generates one list of tokens per
				# post... which may actually be preferable for short
				# 4chan-style posts. But alternatively it could generate one
				# list per sentence - this processor is agnostic in that regard
				self.dataset.update_status("Extracting common phrases from token set %s..." % token_set_name)
				bigram_transformer = Phrases(self.tokens_from_file(temp_file))

				self.dataset.update_status("Training Word2vec model for token set %s..." % token_set_name)
				model = Word2Vec(bigram_transformer[self.tokens_from_file(temp_file)], negative=use_negative, size=dimensionality, sg=use_skipgram, window=window, workers=3, min_count=min_count)

				# save - we only save the KeyedVectors for the model, this
				# saves space and we don't need to re-train the model later
				model_name = token_set_name.split(".")[0] + ".model"
				model.wv.save(str(temp_path.joinpath(model_name)))
				del model
				models += 1

				temp_file.unlink()

		# create another archive with all model files in it
		with zipfile.ZipFile(self.dataset.get_results_path(), "w") as zip:
			for output_path in temp_path.glob("*.model"):
				zip.write(output_path, output_path.name)
				output_path.unlink()

		# delete temporary folder
		shutil.rmtree(temp_path)

		self.dataset.update_status("Finished")
		self.dataset.finish(models)

	def tokens_from_file(self, file):
		"""
		Read tokens from token dump

		If the tokens were saved as JSON, take advantage of this and return
		them as a generator, reducing memory usage and allowing interruption.

		:param Path file:
		:return list:  A set of tokens
		"""
		if file.suffix == "pb":
			with file.open("rb") as input:
				return pickle.load(input)

		with file.open("r") as input:
			input.seek(1)
			while True:
				line = input.readline()
				if line is None:
					break

				if self.interrupted:
					raise ProcessorInterruptedException("Interrupted while reading tokens")

				if line == "]":
					# this marks the end of the file
					raise StopIteration

				try:
					# the tokeniser dumps the json with one set of tokens per
					# line, ending with a comma
					token_set = json.loads(line.strip()[:-1])
					yield token_set
				except json.JSONDecodeError:
					# old-format json dumps are not suitable for the generator
					# approach
					input.seek(0)
					return json.load(input)