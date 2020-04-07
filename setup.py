from setuptools import setup, find_packages

setup(name='pymydao',
      version='0.0.8',
      description='mysql dao',
      url='https://github.com/taohuangtao/pymydao',
      author='huangtao',
      author_email='tao_huangtao@qq.com',
      license='MIT',
      packages=find_packages(),
      install_requires=['pymysql'])
