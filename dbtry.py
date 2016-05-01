import sqlalchemy 
from sqlalchemy import create_engine
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import Sequence
from sqlalchemy import Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy import event

class DbException(Exception): 
    def __init__(self, message = None, errors = None):
        super(DbException, self).__init__(message)
        self.errors = errors 
        if not errors:
            self.errors = {"status_code" : -1} 
 # this sets the default and unused error code.
        self.errors = errors                 


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

# dunno if we are going to use a config file later.
dbpath = '/home/user/ims/bmidb.db'
#Base =  declarative_base()

# This function is symmetric to the way the DB classes work.
# DB classes need to inherit Base (sqlalchemy class),
# which is generated on runtime using
# declarative_base().
# We need to follow the symmetry and generate our class on runtime.
def get_db_classes(Base):
  class ProjectBMI(Base):
    __tablename__ = 'proj_to_network'
    pid = Column(Integer, Sequence('Project_BMI_seq'),\
            primary_key = True) 
    project_name = Column(String(15), unique = True) 
    provision_network = Column(String(15))
  
    def __repr__(self):
      return str({"project_name" :\
          self.project_name, "provision_network"\
              : self.provision_network})

  class Images(Base):
    __tablename__ = "Images"
    pid = Column(Integer, ForeignKey('proj_to_network.pid'))
    filenum = Column(Integer, Sequence("Images_seq"),\
            primary_key = True, unique = True)
    filename = Column(String(50))
#TODO Bug : the tuple (pid, filename) is not unique.
# Change this class to accomodate this constrain.

  class ImageSnaps(Base):
    __tablename__ = "Snaps"
    filenum = Column(Integer,\
             ForeignKey('Images.filenum'))
    snap_name = Column(String(50))
    __table_args__ = (PrimaryKeyConstraint(\
                filenum, snap_name),) 
# the primary key is filename+snapname

  return ProjectBMI, Images, ImageSnaps 

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
  a, b, c = get_db_classes(Base)
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
  

def get_image_list_from_db(project):
  dbo = DbObject() ## gets the db part done. Just run queries! :)

  p_entry  = dbo.session.query(dbo.Project).filter_by(project_name\
           = project).first()
# None as a query result implies that the project does not exist.
  if p_entry is None: 
    raise DbException(message = "No such Project",\
            errors = {"status_code" : 491})

  image_list = [a.filename for a in dbo.session.query(dbo.Images)\
               .filter_by(pid = p_entry.pid).all()]
  return image_list

# This Dbobject abstracts the whole sqlalchemy process of 
# creating engine. Then the base. and then using the base to 
# generate Database classes, and the engine to generate the session.
# Just call the constructor and an object is created that has everything.
# Typically you just need the self.session object to run the queries.
# Refer get_image_list_from_db for an example
class DbObject:
  def __init__(self):
    self.eg, self.base = create_db_engine(getbase = True) 
    self.session = get_session(self.eg)
    self.Project, self.Images, self.Snaps =\
                       get_db_classes(self.base)
  def destroy(self):
    self.session.close()

if __name__ == "__main__":
  test_create() 
  print get_image_list_from_db("bmi_penultimate") 
