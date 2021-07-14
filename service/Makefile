#
# Global Settings
#

INSTALL = install
DESTDIR ?= /
PREFIX  ?= $(DESTDIR)/usr
SYSTEMD_DIR = $(HOME)/.config/systemd/user

BIN_PATH = $(PREFIX)/bin
PATH_EXEC = ${BIN_PATH}/covid_data_update
INFO_EXEC = ${BIN_PATH}/gpu_info.py

#
# Targets
#

all:
	@echo "Nothing to do"

install:
	@echo "sudo ${INSTALL} -m0755 -D covid_data_update.sh ${PATH_EXEC}"
	$(shell sudo ${INSTALL} -m0755 -D covid_data_update.sh ${PATH_EXEC})
	@echo "sudo ${INSTALL} -m0755 -D gpu_info.py ${INFO_EXEC}"
	$(shell sudo ${INSTALL} -m0755 -D gpu_info.py ${INFO_EXEC})
	$(INSTALL) -m0644 -D covid_data_update.service $(SYSTEMD_DIR)/covid_data_update.service
	$(INSTALL) -m0644 -D covid_data_update.timer $(SYSTEMD_DIR)/covid_data_update.timer
	systemctl --user enable covid_data_update.service
	systemctl --user enable covid_data_update.timer
	systemctl --user start covid_data_update.timer

uninstall:
	@echo "sudo rm ${PATH_EXEC}"
	$(shell sudo rm ${PATH_EXEC})
	@echo "sudo rm ${INFO_EXEC}"
	$(shell sudo rm ${INFO_EXEC})
	systemctl --user disable covid_data_update.service
	systemctl --user stop covid_data_update.timer
	systemctl --user disable covid_data_update.timer
	rm $(SYSTEMD_DIR)/covid_data_update.service
	rm $(SYSTEMD_DIR)/covid_data_update.timer

.PHONY: all install uninstall
