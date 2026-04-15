# Claude Code Skills

Личные скилы для Claude Code — синхронизируются между машинами через этот репозиторий.

## Установка на новой машине

```bash
# Если папки ~/.claude/skills/ ещё нет
git clone <URL этого репо> ~/.claude/skills

# Если папка существует и пуста
cd ~/.claude/skills
git clone <URL этого репо> .

# Если уже есть какие-то скилы — перенеси их во временную папку,
# клонируй, потом верни обратно и закоммить
```

После клонирования перезапусти Claude Code — скилы подхватятся автоматически.

## Добавление нового скила

```bash
cd ~/.claude/skills
mkdir my-new-skill
# создай my-new-skill/SKILL.md с frontmatter (name, description) + инструкции
git add my-new-skill/
git commit -m "add my-new-skill"
git push
```

## Синхронизация

```bash
cd ~/.claude/skills
git pull   # подтянуть изменения с других машин
git push   # отправить локальные изменения
```

## Доступные скилы

- **ai-research** — ресерч актуальных событий в мире ИИ (релизы моделей, исследования, индустрия).
