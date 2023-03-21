
<div style="text-align: center">
    <span style="font-size: 3em; font-weight: 700; font-family: Consolas">
        Lab 02 <br>
        Mapreduce programing
    </span>
    <br><br>
    <span style="">
        A assignment for <code>CSC14118</code> Introduction to Big Data @ 20KHMT1
    </span>
</div>

## Collaborators (The girls)

- `20127011` **Lê Tấn Đạt** ([@githubaccount1](https://github.com/callmetandat))
- `20127438` **Lê Nguyễn Nguyên Anh** ([@githubaccount2](https://github.com/nnaaaa))
- `20127458` **Đặng Tiến Đạt** ([@githubaccount3](https://github.com/TienDat57))
- `20127627` **Nguyễn Quốc Thắng** ([@githubaccount4](https://github.com/nthanq2505))

## Instructors

- `HCMUS` **Đoàn Đình Toàn** ([@ddtoan](ddtoan18@clc.fitus.edu.vn))
- `HCMUS` **Nguyễn Ngọc Thảo** ([@nnthao](nnthao@fit.hcmus.edu.vn))

---
<div style="page-break-after: always"></div>

## Quick run
>
> You can clear this section and insert your own instruction.

To export your report with the [OSCP](https://help.offensive-security.com/hc/en-us/articles/360046787731-PEN-200-Reporting-Requirements) template, you should install the following packages:

For Archlinux:

```bash
pacman -S texlive-most pandoc
```

For Ubuntu:

```
apt install texlive-latex-recommended texlive-fonts-extra texlive-latex-extra pandoc
```

Then using the `convert_md_to_pdf.sh` to export your report to pdf.

> For those who don't want to use OSCP template, you can use alternative ways to export your `report.md` to `pdf` (`Typora`, `pandoc` without `Latex`, `Obsidian`,...) but please keep the `yaml` header of the report as follow:

```yaml
---
title: "Lab 01: A Gentle Introduction to Hadoop"
author: ["your-team-name"]
date: "yyyy-mm-dd"
subtitle: "CSC14118 Introduction to Big Data 20KHMT1"
lang: "en"
titlepage: true
titlepage-color: "0B1887"
titlepage-text-color: "FFFFFF"
titlepage-rule-color: "FFFFFF"
titlepage-rule-height: 2
book: true
classoption: oneside
code-block-font-size: \scriptsize
---
```