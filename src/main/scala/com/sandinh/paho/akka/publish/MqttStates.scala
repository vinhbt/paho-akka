package com.sandinh.paho.akka.publish

//++++ FSM states ++++//
sealed trait MqttState
case object DisconnectedState extends MqttState
case object ConnectedState extends MqttState
case object ConnectingState extends MqttState
