import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import *
from src.utils import *

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()