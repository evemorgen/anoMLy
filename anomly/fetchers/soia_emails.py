import email
import logging
import re
from base64 import b64decode
from imaplib import IMAP4_SSL
from luigi import Parameter, DateParameter
from luigi.contrib import sqla
from sqlalchemy import BigInteger, Integer, bindparam
from datetime import datetime

import dateparser


logger = logging.getLogger('soia_emails_logger')


def create_imap_client(email_address, password):
    # create imap client and select inbox
    imap_client = IMAP4_SSL('outlook.office365.com')
    imap_client.login(email_address, password)
    imap_client.select("INBOX/soia")
    return imap_client


def remove_occurances(string, things_to_remove):
    for thing in things_to_remove:
        string = string.replace(thing, "")
    return string


def unbase64_content(content):
    if isinstance(content, list):
        try:
            return b64decode("".join([value.get_payload() for value in content])).decode()
        except UnicodeDecodeError:
            logger.warning("coudn't unbase64 thing, skipping, content: %s", content)
            raise ValueError("Bad base64 data from email")
    else:
        return content


def deduplicated(list_of_things):
    return list(set(list_of_things))


class SoiaEmailFetcher(sqla.CopyToTable):
    email_address = Parameter()
    password = Parameter()
    date = DateParameter()

    columns = [
        (["id", Integer()], {"autoincrement": True, "primary_key": True}),
        (["start", BigInteger()], {}),
        (["end", BigInteger()], {}),
        (["insert_date", BigInteger()], {})
    ]
    connection_string = "sqlite:///data/soia_email.db"
    table = "soia"
    regexes = [
        (r"<b>Duration:</b>.*<br>", ["<b>Duration:</b>", "<br>"]),
        (r"(\d{2,4}.){2,4}.*<o", ["<o"])
    ]

    def rows(self):
        for start, end in deduplicated(self.generate_rows()):
            yield "auto", start, end, datetime.now().strftime('%s')

    def copy(self, conn, ins_rows, table):
        bound_cols = dict((c, bindparam("_" + c.key)) for c in table.columns if c.key != "id")
        ins = table.insert().values(bound_cols)
        conn.execute(ins, ins_rows)

    def generate_rows(self):
        imap_client = create_imap_client(self.email_address, self.password)
        try:
            code, data = imap_client.search(None, "ALL")

            soia_timestamps = []
            # iterate over emails
            for number in data[0].split(b" "):
                code, data = imap_client.fetch(number, '(RFC822)')
                message = email.message_from_string(data[0][1].decode())

                # get actual email content
                date = dateparser.parse(message["Date"])
                content = message.get_payload()

                # handle base64 content
                try:
                    unbased_content = unbase64_content(content)
                except ValueError:
                    continue

                # iterate over regexes trying to match date
                for regex, replaces in self.regexes:
                    match = re.search(regex, unbased_content)
                    if match is not None:
                        dates = remove_occurances(match.group(), replaces)

                        start, end = dates.rsplit("-", maxsplit=1)
                        if " " not in end.strip():
                            end = f"{date.year}-{date.month}-{date.day} {end}"

                        parsed_start = dateparser.parse(start)
                        parsed_end = dateparser.parse(end)
                        if parsed_end is None or parsed_start is None:
                            logger.warning("coudn't parse the following: %s", match)
                            continue
                        row = (
                            parsed_start.strftime('%s'),
                            parsed_end.strftime('%s')
                        )
                        logger.debug("Adding the following row: %s", row)
                        soia_timestamps.append(row)
                        break
        except Exception as err:
            logger.error("Something went terribly wrong! %s", err)

        finally:
            imap_client.close()
            imap_client.logout()
        return soia_timestamps
