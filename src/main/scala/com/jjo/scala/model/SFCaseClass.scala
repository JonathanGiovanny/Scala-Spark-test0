package com.jjo.scala.model

final case class SF(name: String, id: String, market: String, contacts: List[Contact])
final case class Contact(id: String = null, name: String, email: String, region: String = null, title: String = null, phone: String)
