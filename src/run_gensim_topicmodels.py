import datetime
import os
import pickle
import gensim
import numpy
from convert_points_to_grid import define_spatiotemporal_unit, define_spatiotemporal_unit_topic, raw_txy_to_index_txy
from src.convert_tweet_to_gensim import create_twitter_text_files
import settings as s

# Logging ver. 2016-05-20
from logging import handlers
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.handlers.RotatingFileHandler('log.log', maxBytes=1000000, backupCount=3)  # file handler
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()  # console handler
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.info('Initializing.')

def iter_docs(topdir, stoplist):

    writer = open(os.path.join(MODELS_DIR, MODEL_NAME+"_docnames.csv"), 'w', newline='', encoding="utf-8")
    for fn in os.listdir(topdir):
        basename = basename = os.path.basename(fn)
        name, ext = os.path.splitext(basename)
        writer.write(name+'\n')
        fin = open(os.path.join(topdir, fn), 'r')
        text = fin.read()
        fin.close()
        yield (x for x in
               gensim.utils.tokenize(text, lowercase=True, deacc=True,
                                     errors="ignore")
               if x not in stoplist)
    writer.close()

class MyCorpus(object):

    def __init__(self, topdir, stoplist):
        self.topdir = topdir
        self.stoplist = stoplist
        self.dictionary = gensim.corpora.Dictionary(iter_docs(topdir, stoplist))

    def __iter__(self):
        for tokens in iter_docs(self.topdir, self.stoplist):
            yield self.dictionary.doc2bow(tokens)

def topic_to_grid(st_units_topic, model, model_corpus, outfile_1, outfile_2):
    density = numpy.zeros(st_units_topic)
    density_2 = numpy.zeros(st_units_topic)
    with open(DOCNAMES, encoding="utf-8") as f:
        i = 0
        for line in f:
            # logger.debug(line)
            datehour, longitude, latitude = line.rstrip().split('_')
            datestring = datehour[0:10]
            houstring = datehour[11:13]
            t = datestring + " " + houstring + ":00:00"
            x = (int(longitude)*0.01) + 0.001
            y = (int(latitude)*0.01) + 0.001
            # logger.debug(t)
            # logger.debug(x)
            # logger.debug(y)

            vector = model_corpus[i]
            # logger.debug(i)
            # logger.debug(vector)
            try:
                if len(vector):
                    index = raw_txy_to_index_txy(timestart, timeend, aoi, unit_temporal, unit_spatial, t, x, y)
                    problist = refer_doctopic_matrix(model, vector)
                    # logger.debug(problist)
                    # logger.debug(index)
                    # read topic
                    for j in problist:
                        if j[0] < 10:
                            # logger.debug(j)
                            # logger.debug(index)
                            density[index[0], index[1], index[2], j[0]] = j[1]
                        if 10 <= j[0] < 20:
                            # logger.debug(j)
                            # logger.debug(index)
                            density_2[index[0], index[1], index[2], j[0]-10] = j[1]

            except IndexError as err:
                logger.debug(t, err)
                continue
            finally:
                i = i + 1
    output = open(outfile_1, 'wb')
    logger.debug("saving.")
    pickle.dump(density, output, protocol=4)
    output.close()
    output_2 = open(outfile_2, 'wb')
    logger.debug("saving.")
    pickle.dump(density_2, output_2, protocol=4)
    output_2.close()


def refer_doctopic_matrix(model, vector):
    problist = []
    problist = model[vector]
    # logger.debug(problist)
    return problist

if __name__ == '__main__':
    now = datetime.datetime.now()
    #
    # # Retrieval parameters
    # timestart = "2013-07-01 00:00:00"
    # timestart_text = "20130701"
    # timeend = "2013-07-31 23:59:59"
    # timeend_text = "20130731"
    # # Area of interest
    # # aoi = [122.933198,24.045416,153.986939,45.522785] # Japan
    # aoi = [138.72, 34.9, 140.87, 36.28]  # Greater Tokyo Area
    #
    # unit_temporal = 60  # in minutes
    # unit_spatial = 0.01  # in degrees
    #
    # num_topic = 50

    engine_conf = s.ENGINE_CONF
    table = s.TABLE

    timestart = s.TIMESTART
    timestart_text = s.TIMESTART_TEXT
    timeend = s.TIMEEND
    timeend_text = s.TIMEEND_TEXT
    aoi = s.AOI
    unit_temporal = s.UNIT_TEMPORAL
    unit_spatial = s.UNIT_SPATIAL
    num_topic = s.NUM_TOPIC

    # Parameter setting
    # MODELS_DIR = "/Users/koitaroh/Documents/Data/Experiments/Experiment_" + now.strftime("%Y%m%d_%H%M%S")
    MODELS_DIR = s.MODELS_DIR
    MODEL_NAME = s.MODEL_NAME
    MALLET_FILE = s.MALLET_FILE
    TEXTS_DIR = s.TEXTS_DIR
    stoplist = s.STOPLIST
    DOCNAMES = s.DOCNAMES
    tweet_df_name = s.TWEET_DF_NAME
    tweet_counter_file = s.TWEET_COUNTER_FILE
    LSI_counter_file = s.LSI_counter_file_1
    LSI_counter_file_2 = s.LSI_counter_file_2
    LDA_counter_file = s.LDA_counter_file_1
    LDA_counter_file_2 = s.LDA_counter_file_2
    HDP_counter_file = s.HDP_counter_file_1
    HDP_counter_file_2 = s.HDP_counter_file_2


    outdir = s.OUTDIR


    st_units = define_spatiotemporal_unit(timestart, timeend, aoi, unit_temporal, unit_spatial)
    st_units_topic = define_spatiotemporal_unit_topic(timestart, timeend, aoi, unit_temporal, unit_spatial, 10)
    logger.debug(st_units_topic)
    create_twitter_text_files(engine_conf, table, st_units, tweet_counter_file, aoi, timestart, timeend, tweet_df_name, outdir, unit_temporal, unit_spatial)

    # Create corpus (MMcorpus)
    corpus = MyCorpus(TEXTS_DIR, stoplist)
    corpus.dictionary.save(os.path.join(MODELS_DIR, MODEL_NAME+"_mt.dict"))
    corpus.dictionary.save_as_text(os.path.join(MODELS_DIR, MODEL_NAME+"_text.dict"))
    gensim.corpora.MmCorpus.serialize(os.path.join(MODELS_DIR, MODEL_NAME+"_mt.mm"),
                                      corpus)
    corpus_mm = gensim.corpora.MmCorpus(os.path.join(MODELS_DIR, MODEL_NAME+"_mt.mm"))
    # print(corpus_mm)

    # TFIDF
    logger.info('Transforming to TFIDF')
    # LSI, to start, transform bow corpus to tfidf corpus
    tfidf = gensim.models.TfidfModel(corpus_mm)  # step 1 -- initialize a model
    corpus_tfidf = tfidf[corpus_mm]

    # LSI
    logger.info('Starting LSA')
    lsi = gensim.models.LsiModel(corpus_tfidf, id2word=corpus.dictionary, num_topics=num_topic)  # initialize an LSI transformation
    lsi.save(os.path.join(MODELS_DIR, MODEL_NAME+".lsi"))  # same for tfidf, lda, ...
    # print(lsi[corpus_tfidf])
    corpus_lsi = lsi[corpus_tfidf]  # create a double wrapper over the original corpus: bow->tfidf->fold-in-lsi
    print(lsi.show_topics(num_topic))
    topic_to_grid(st_units_topic, lsi, corpus_tfidf, LSI_counter_file, LSI_counter_file_2)

    # LDA
    logger.info('Starting LDA')
    lda = gensim.models.LdaModel(corpus_mm, id2word=corpus.dictionary, num_topics=num_topic, alpha='auto')
    lda.save(os.path.join(MODELS_DIR, MODEL_NAME+".lda"))
    corpus_lda = lda[corpus_mm]
    # change formatted to False to retrieve list
    print(lda.show_topics(num_topic, formatted=True))
    topic_to_grid(st_units_topic, lda, corpus_mm, LDA_counter_file, LDA_counter_file_2)


    # LDA with Mallet
    logger.info("Starting LDA with Mallet")
    lda_mallet = gensim.models.wrappers.LdaMallet(MALLET_FILE, corpus=corpus_mm, num_topics=num_topic, id2word=corpus.dictionary)
    lda_mallet.save(os.path.join(MODELS_DIR, MODEL_NAME+".lda_mallet"))
    print(lda_mallet.show_topics(num_topics=num_topic, num_words=10, formatted=True))

    # # HDP
    # logger.info('Starting HDP')
    # hdp = gensim.models.HdpModel(corpus_mm, id2word=corpus.dictionary)
    # hdp.save(os.path.join(MODELS_DIR, MODEL_NAME+".hdp"))
    # print(hdp.show_topics(topics=-1, topn=10))
    # topic_to_grid(st_units_topic, hdp, corpus_mm, HDP_counter_file, HDP_counter_file_2)


