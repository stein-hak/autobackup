import configparser



class my_conf:

	def __init__(self,conf_file):
		self.conf = configparser.SafeConfigParser()
		self.conf.read(conf_file)

	def get_sections(self):
		return self.conf.sections()

	def get(self,section,option,fallback=None):
		try:
			ret = self.conf.get(section,option)
		except:
			ret = fallback
		return ret

	def getint(self,section,option,fallback=None):
		try:
			ret = self.conf.getint(section,option)
		except:
			ret = fallback

		return ret

	def getboolean(self,section,option,fallback=None):
		try:
			ret = self.conf.getboolean(section,option)
		except:
			ret = fallback
		return ret

	def getfloat(self, section, option, fallback=None):
		try:
			ret = self.conf.getfloat(section, option)
		except:
			ret = fallback
		return ret
