// Ekip bilgisi tek yerde tanımlı; isimler büyük/küçük harf dönüşümü yapılmadan gösterilir.
export const MEMBERS = [
  { name: 'Emir Han GÜRDAMAR', no: '233311075', roles: ['Öğrenci 1'] },
  { name: 'İsmail Esat UÇMAK', no: '233311071', roles: ['Öğrenci 2'] },
  { name: 'Mehmet Buğra TEKİN', no: '233311074', roles: ['Öğrenci 3'] },
  { name: 'Hüseyin GÜLCAN', no: '233311070', roles: ['Öğrenci 4', 'Öğrenci 5'] },
]

const byRole = (role) => MEMBERS.find((m) => m.roles.includes(role))

export const STUDENTS = [1, 2, 3, 4, 5].map((n) => ({ n, role: `Öğrenci ${n}`, ...byRole(`Öğrenci ${n}`) }))
