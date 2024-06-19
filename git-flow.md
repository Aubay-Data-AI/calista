## Git Flow step-by-step

Pour développer une nouvelle tâche, crééz une nouvelle branche depuis la branche main et commencez vos développements dessus :
```git checkout -b <nom_branche>```

Lorsque vous avez terminé vos développements, réalisez un rebase interactif avec tous vos nouveaux commits afin de les squasher en un seul commit.
Pour ce faire, placez vous sur la branche depuis laquelle vous avez démarré vos développements et copiez le hash du commit le plus ancien à partir
duquel vous souhaitez commencer votre rebase interactif (avec un git log par exemple). Puis exécutez la commande :
```git rebase -i <hash_dernier_commit>```

Vous arrivez ensuite dans la console VIM. Appuyez sur _i_ afin de rentrer en mode interactif puis remplacez les _pick_ de tous les commits, excepté le premier, par des _s_.
Quittez le mode interactif puis quittez la console VIM en sauvegardant (avec un _:wq!_ par exemple).
Vous arrivez ensuite dans une deuxième console VIM qui vous demandera de nommer le nom du commit final.
En entrant de nouveau en mode interactif, renommez le commit puis quittez la console en sauvegardant.

Afin de mettre à jour votre branche avec la branche main, mettez à jour votre branche main puis effectuez un rebase depuis cette branche main :
```
git fetch origin main:main
git rebase main
```

Résolvez les potentiels conflits générés par le rebase

Envoyez votre nouvel historique de commit vers la branche remote :
```git push -f```

Vous pouvez enfin créer la pull request afin de merger votre branche vers la branche main
