#!/bin/bash

usage(){

cat << EOF
OPTIONS:
   -h      Show this message
   -s      Start example
   -q      Number of tables and quorum
   -r      Replication factor
   -c      Start cluster manager
   -d      Start node
   -w      Start manager watcher
EOF

}
quorum=4
replication=2


startExample(){
  initClusterManager
  sleep 1
  initManagerWatcher
  sleep 1
  for (( i = 0; i < quorum; i++ )); do
      initNode
      sleep 1
  done
}

initNode(){
  start java -cp  target/DHT_DSCC-1.0-SNAPSHOT-jar-with-dependencies.jar es.upm.dit.dscc.DHT.DHT
}

initClusterManager(){
  start java -cp  target/DHT_DSCC-1.0-SNAPSHOT-jar-with-dependencies.jar es.upm.dit.dscc.DHT.ClusterManager $quorum $replication
}

initManagerWatcher(){
  start java -cp  target/DHT_DSCC-1.0-SNAPSHOT-jar-with-dependencies.jar es.upm.dit.dscc.DHT.ManagerWatcher $quorum $replication
}

start() {
  gnome-terminal -- "$@"
}


if [ "$#" -eq 0 ]
then
  startExample
elif [ "$#" -eq 1 ]
  then
  getopts “hscdw” OPTION
    case $OPTION in
      h)
        usage
        exit 1
        ;;
      d)
        initNode
        ;;
      c)
        initClusterManager
        ;;
      w)
        initManagerWatcher
      	;;
     s)
        startExample
      	;;
      ?)
        usage
        exit
        ;;
    esac
  else
    while getopts “:q:r:hscdw” OPTION
    do
      case $OPTION in
        q)
          echo "$OPTARG"
          quorum=$OPTARG
          ;;
        r)
          echo "$OPTARG"
          replication=$OPTARG
          ;;
        h)
          usage
          exit 1
          ;;
        d)
          initNode
          ;;
        c)
          initClusterManager
          ;;
        w)
          initManagerWatcher
        	;;
       s)
          startExample
        	;;
        ?)
          usage
          exit
          ;;
      esac
    done
fi