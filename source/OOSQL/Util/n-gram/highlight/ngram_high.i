%module ngram_high

extern char * swig_web_highlight_ngram(char *result_val, char *pat_val);
extern char * swig_get_stopwords(char* input);
extern char * swig_make_n_gram(char * input);
extern char * swig_cut_string_by_query(const char *src, const char *pat_val, int max_len);
