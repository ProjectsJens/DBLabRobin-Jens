{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {
    "collapsed": false
   },
   "outputs": [
    {
     "ename": "IOError",
     "evalue": "[Errno 2] No such file or directory: '/insider_movies_label.pkl'",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mIOError\u001b[0m                                   Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-5-f319d44d728f>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[1;32m      2\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0mpickle\u001b[0m \u001b[0;32mimport\u001b[0m \u001b[0mload\u001b[0m \u001b[0;32mas\u001b[0m \u001b[0mpklLoad\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      3\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 4\u001b[0;31m \u001b[0mlabel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mpklLoad\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mopen\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mKafkaConstants\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mLABEL_PATH\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0;34m\"r\"\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m      5\u001b[0m \u001b[0mmodel\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mpklLoad\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mopen\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mKafkaConstants\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mMODEL_PATH\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0;34m\"r\"\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      6\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;31mIOError\u001b[0m: [Errno 2] No such file or directory: '/insider_movies_label.pkl'"
     ]
    }
   ],
   "source": [
    "import KafkaConstants\n",
    "from pickle import load as pklLoad\n",
    "\n",
    "label = pklLoad(open(KafkaConstants.LABEL_PATH, \"r\"))\n",
    "model = pklLoad(open(KafkaConstants.MODEL_PATH, \"r\"))\n",
    "\n",
    "print label\n",
    "print model.predict([[12,12,2313]])"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 2",
   "language": "python",
   "name": "python2"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 2
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython2",
   "version": "2.7.14"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
