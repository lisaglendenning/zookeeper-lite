#!/usr/bin/env python

##############################################################################
# This file was automatically generated by Maven at ${main.timestamp}
#
# Description: Python front end script to ${main.class}
##############################################################################

import sys, os, shlex

def main(argv, environ, env_prefix, main_class):
    prefix = environ.get(
                env_prefix + 'PREFIX',
                os.path.abspath(os.path.dirname(os.path.dirname(
                            os.path.realpath(__file__)))))
    lib = environ.get(
                env_prefix + 'LIB',
                os.path.join(prefix, 'lib'))
    etc = environ.get(
                env_prefix + 'ETC',
                os.path.join(prefix, 'etc'))

    java_classpath = os.path.join(lib, "*")
    java_args = ['-classpath', java_classpath]
    
    log_config = environ.get(env_prefix + 'LOG_CONFIG')
    if not log_config:
        for candidate in 'log4j2-test.xml', \
                'log4j2.xml':
            f = os.path.join(etc, candidate)
            if os.path.isfile(f):
                log_config = f
                break
    if log_config:
        java_args.append("-Dlog4j.configurationFile=%s" % log_config)
            
    config_file = environ.get(env_prefix + 'CONFIG_FILE')
    if not config_file:
        for candidate in 'application.conf', \
                'application.json', \
                'application.properties':
            f = os.path.join(etc, candidate)
            if os.path.isfile(f):
                config_file = f
                break
    if config_file:
        java_args.append("-Dconfig.file=%s" % config_file)
        
    if 'JAVA_ARGS' in environ:
        java_args.extend(shlex.split(environ['JAVA_ARGS']))

    java = os.path.join(environ['JAVA_HOME'], 'bin', 'java') \
            if 'JAVA_HOME' in environ else 'java'
    
    args = [java] + java_args + [main_class] + argv[1:]
    os.execvp(java, args)

if __name__ == "__main__":
    main(sys.argv, os.environ, '${main.prefix}', '${main.class}')