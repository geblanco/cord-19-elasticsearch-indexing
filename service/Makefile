#
# Global Settings
#

INSTALL = install
DESTDIR ?= /
PREFIX  ?= $(DESTDIR)/usr
SYSTEMD_DIR = $(HOME)/.config/systemd/user

BIN_PATH = $(PREFIX)/bin
PATH_EXEC = ${BIN_PATH}/gpu_stats
INFO_EXEC = ${BIN_PATH}/gpu_info.py

#
# Targets
#

all:
	@echo "Nothing to do"

install:
	@echo "sudo ${INSTALL} -m0755 -D gpu_stats.sh ${PATH_EXEC}"
	$(shell sudo ${INSTALL} -m0755 -D gpu_stats.sh ${PATH_EXEC})
	@echo "sudo ${INSTALL} -m0755 -D gpu_info.py ${INFO_EXEC}"
	$(shell sudo ${INSTALL} -m0755 -D gpu_info.py ${INFO_EXEC})
	$(INSTALL) -m0644 -D gpu_stats.service $(SYSTEMD_DIR)/gpu_stats.service
	$(INSTALL) -m0644 -D gpu_stats.timer $(SYSTEMD_DIR)/gpu_stats.timer
	systemctl --user enable gpu_stats.service
	systemctl --user enable gpu_stats.timer
	systemctl --user start gpu_stats.timer

uninstall:
	@echo "sudo rm ${PATH_EXEC}"
	$(shell sudo rm ${PATH_EXEC})
	@echo "sudo rm ${INFO_EXEC}"
	$(shell sudo rm ${INFO_EXEC})
	systemctl --user disable gpu_stats.service
	systemctl --user stop gpu_stats.timer
	systemctl --user disable gpu_stats.timer
	rm $(SYSTEMD_DIR)/gpu_stats.service
	rm $(SYSTEMD_DIR)/gpu_stats.timer

.PHONY: all install uninstall
