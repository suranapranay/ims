import sqlalchemy 
from sqlalchemy import create_engine
import os


dbpath = '/home/user/ims/bmidb' # dunno if we are going to use a config file later.


def is_db():
  global dbpath
  if os.path.isfile(dbpath):
    return True
  return False


def create_db_engine(getbase = False, debug = True):
  from sqlalchemy.ext.declarative import declarative_base
  q_path = 'sqlite://' + dbpath
  if debug:
    print q_path
  eg = create_engine(q_path)
  if getbase:
    Base = declarative_base()
  return eg, Base 

def create_db(debug = False):
  from sqlalchemy import ForeignKey
  from sqlalchemy import Sequence
  from sqlalchemy import Column, Integer, String
  global dbpath

  if is_db():
    return False

  eg, Base = create_db_engine(getbase = True)

  class Project_BMI(Base):
    __tablename__ = 'proj_to_network'
    pid = Column(Integer, Sequence('Project_BMI_seq'), primary_key = True) 
    project_name = Column(String(15), unique = True) 
    provision_network = Column(String(15))

    def __repr__(self):
      return {"pid" : pid, "project_name" :\
          project_name, "provision_network" : provision_network}

  class images(Base):
    __tablename__ = "images"
    pid = Column(Integer, ForeignKey(Project_BMI.pid))
    filenum = Column(Integer, Sequence("images_seq"),\
            primary_key = True)
    filename = Column(String(50))
  try:
    Base.metadata.create_all(eg) 
  except Exception as e:
    return False
 
  if is_db():
    return True

  return False    

def test_create():
  create_db(debug = True)
  
 
