"""
modbpm.messages
===============
"""

ACT_MODEL_NOT_EXIST = "act_model does not exist: %(message)s"


def build_message(tmpl, args):
    if isinstance(args, dict):
        message = ', '.join(map(lambda x: "%s[%r]" % x, args.iteritems()))
    else:
        message = str(args)

    return tmpl % locals()
