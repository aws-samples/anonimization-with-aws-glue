import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]

## A definição de "args" abaixo é para ser usada no notebook:
##args = {'JOB_NAME':'anonimizacao'}
## A definição de "args" abaixo é para ser usada no script do Job do Glue:
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Caminho de output para armazenamento do arquivo anonimizado no S3
## TODO: substituir pelo caminho do seu bucket
s3_output_path = "s3://<<caminho onde o arquivo final será armazenado>>"


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "pep-original", table_name = "paciente"]
## @return: datasource
## @inputs: []
datasource = glueContext.create_dynamic_frame.from_catalog(database = "pep-original", table_name = "paciente")
## @type: ApplyMapping
## @args: [mapping = [("nome", "string", "nome", "string"), ("cpf", "string", "cpf", "string"), ("email", "string", "email", "string"), ("cep", "string", "cep", "string"), ("rua", "string", "rua", "string"), ("numero", "long", "numero", "long"), ("cidade", "string", "cidade", "string"), ("estado", "string", "estado", "string"), ("nascimento", "string", "nascimento", "string"), ("peso", "int", "peso", "int")]]
## @return: applymapping
## @inputs: [frame = datasource]
applymapping = ApplyMapping.apply(frame = datasource, mappings = [("nome", "string", "nome", "string"), ("cpf", "string", "cpf", "string"), ("email", "string", "email", "string"), ("cep", "string", "cep", "string"), ("rua", "string", "rua", "string"), ("numero", "long", "numero", "long"), ("cidade", "string", "cidade", "string"), ("estado", "string", "estado", "string"), ("nascimento", "string", "nascimento", "string"), ("peso", "long", "peso", "long")])
## @type: ResolveChoice
## @args: [choice = "make_struct"]
## @return: originalData
## @inputs: [frame = applymapping]
originalData = ResolveChoice.apply(frame = applymapping, choice = "make_struct")



supressao = DropFields.apply(frame = originalData, 
                             paths=['rua', 'numero'])
                             
def RecuperaUltimoSobrenome(rec):
    nomeCompleto = rec["nome"]
    nomeSeparado = nomeCompleto.split()
    rec["nome"] = nomeSeparado[len(nomeSeparado)-1]
    return rec

nomeGeneralizado = Map.apply(frame = supressao, 
                             f = RecuperaUltimoSobrenome)
                             
def RecuperaDominioEmail(rec):
    emailCompleto = rec["email"]
    dominioEmail = emailCompleto.split("@")
    rec["email"] = dominioEmail[1]
    return rec

emailGeneralizado = Map.apply(frame = nomeGeneralizado, 
                              f = RecuperaDominioEmail)
                              

def GeneralizaCep(rec):
    cepOriginal = rec["cep"]
    rec["cep"] = cepOriginal[0:5]
    return rec

cepGeneralizado = Map.apply(frame = emailGeneralizado, 
                            f = GeneralizaCep)
                            

def AnoNascimento(rec):
    nascimentoOriginal = rec["nascimento"]
    anoNascimento = nascimentoOriginal.split("/")
    rec["nascimento"] = anoNascimento[2]
    return rec

anoNascimento = Map.apply(frame = cepGeneralizado, 
                          f = AnoNascimento)
                          
                          

def Round(x, base=5):
    return base * round(x/base)

def AddRuidoPeso(rec):
    pesoOriginal = rec["peso"]
    rec["peso"] = Round(pesoOriginal)
    return rec

pesoArredondado = Map.apply(frame = anoNascimento, 
                            f = AddRuidoPeso)
                            
                            
import random
import hashlib


def HashCPF(rec):
    cpfOriginal = str(rec["cpf"]) + str(random.randrange(100,999))
    rec["cpf"] = hashlib.sha256(cpfOriginal.encode()).hexdigest()
    return rec

cpfHasheado = Map.apply(frame = pesoArredondado, 
                        f = HashCPF)
                        
                        
## @args: [connection_type = "s3", connection_options = {"path": s3_output_path}, format = "parquet"]
## @inputs: [frame = cpfHasheado]

glueContext.write_dynamic_frame.from_options(frame = cpfHasheado, 
                                             connection_type = "s3", 
                                             connection_options = {"path": s3_output_path},
                                             format = "parquet")
job.commit()