import sqlalchemy 
from sqlalchemy import create_engine
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from sqlalchemy import Column, Integer, String
 
dbpath = '/home/user/ims/bmidb.db' # dunno if we are going to use a config file later.
Base =  declarative_base()


def get_db_classes(Base):
  class Project_BMI(Base):
    __tablename__ = 'proj_to_network'
    pid = Column(Integer, Sequence('Project_BMI_seq'), primary_key = True) 
    project_name = Column(String(15), unique = True) 
    provision_network = Column(String(15))
  
    def __repr__(self):
      return {"pid" : pid, "project_name" :\
          project_name, "provision_network" : provision_network}
  
  class Images(Base):
    __tablename__ = "Images"
    pid = Column(Integer, ForeignKey(Project_BMI.pid))
    filenum = Column(Integer, Sequence("Images_seq"),\
            primary_key = True)
    filename = Column(String(50))

  return Project_BMI, Images 

def is_db():
  global dbpath
  if os.path.isfile(dbpath):
    return True
  return False


def create_db_engine(getbase = False, debug = False):
  from sqlalchemy.ext.declarative import declarative_base
  q_path = 'sqlite:///' + dbpath
  Base = None
  if debug:
    print q_path
  eg = create_engine(q_path)
  if getbase:
    Base = declarative_base()
  return eg, Base 

def create_db(debug = False):
  global dbpath
  if is_db():
    return False
  eg, Base = create_db_engine(getbase = True)
  a, b = get_db_classes(Base)
  try:
    Base.metadata.create_all(eg) 
  except Exception as e:
    return e
  if is_db():
    return True

  return False    



def get_session(eg):
  from sqlalchemy.orm import sessionmaker
  Session = sessionmaker(bind = eg)
  return Session()

def test_create():
  print create_db(debug = True)
  

def get_image_list_from_db(usr, passwd, project):
  eg, base = create_db_engine(getbase = True) 
  session = get_session(eg)
  Project, Images = get_db_classes(base)
  image_list = session.query(Project).filter("project_name")
  return image_list


if __name__ == "__main__":
  #test_create() 
  print get_image_list_from_db("bb","cc","bmi").all() 
