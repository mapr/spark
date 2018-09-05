#!/usr/bin/env bash


function createUserGroups() {
  groups=($USER_GROUPS)
  groupIds=($USER_GROUPS_IDS)

  for i in "${!groups[@]}"
  do
    groupadd -f -g ${groupIds[i]} ${groups[i]}
    usermod -a -G  ${groups[i]} $CURRENT_USER
  done
}

function createUser() {
  if ! id $CURRENT_USER >/dev/null 2>&1; then
    adduser -u $USER_ID $CURRENT_USER -m -d /home/$CURRENT_USER
    if [ -d /home/$CURRENT_USER ]; then
      cd /home/$CURRENT_USER
    fi
  fi
  chown $CURRENT_USER ./
}

function copySecurity() {
  if [ ! -z "$MAPR_SSL_LOCATION" ] ; then
    if [ -z "$MAPR_HOME" ] ; then
      MAPR_HOME=/home/mapr
    fi
    cp "$MAPR_SSL_LOCATION"/* $MAPR_HOME/conf/
  fi
}
