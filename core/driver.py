import sys
import os

def main():
  t = os.listdir('feature')
  print(t)
  #sys.path.append('.')
  sys.path.append('./feature/phone_features')
  module = __import__('phone')
  #module = __import__('./feature/phone_features/')

main()
